import os
import pyodbc


def connect_sql_server():
    """Cria ligação ao SQL Server usando variáveis de ambiente."""
    try:
        conn = pyodbc.connect(
            "DRIVER={SQL Server};"
            f"SERVER={os.getenv('SQL_SERVER')};"
            f"DATABASE={os.getenv('SQL_DATABASE')};"
            "Trusted_Connection=yes;"
        )
        return conn.cursor()
    except pyodbc.Error as e:
        print(f"Error connecting to SQL Server: {e}")
        return None


def parse_result(result):

    if result[26]:
        flight = result[26]
    elif result[23]:
        flight = result[23]
    elif result[20]:
        flight = result[20]
    else:
        flight = result[18]

    return {
        "uat_number": result[0],
        "expedition_date": result[13],
        "consignment": result[15],
        "arrival_date": result[29],
        "flight": flight,
        "carrier": flight,
        "arrival_port": result[3],
    }


def fetch_last_resultset_row(cursor):
    """
    Avança pelos múltiplos result sets até encontrar um com resultados.
    """
    while True:
        try:
            row = cursor.fetchone()
            if row:
                return row
        except pyodbc.ProgrammingError:
            pass

        if not cursor.nextset():
            return None


def get_sql_data(tracking_number):
    cursor = connect_sql_server()
    if not cursor:
        return None

    try:
        # -----------------------------------------------------
        # 1) SELECT complexo com variáveis
        # -----------------------------------------------------
        select_query = """
            DECLARE
                  @DT_FROM DATETIME = DATEADD(DAY, -30, GETDATE()),
                  @DT_TO   DATETIME = DATEADD(DAY, +10, GETDATE()),
                  @object_num VARCHAR(29) = ?,
                  @MAIL_CLASS_NM CHAR(1) = NULL,
                  @MAIL_SUBCLASS_NM VARCHAR(2) = NULL,
                  @uat_number VARCHAR(29) = NULL,
                  @expedition_date DATETIME = NULL,
                  @consignment VARCHAR(29) = NULL,
                  @flight VARCHAR(29) = NULL,
                  @arrival_date DATETIME = NULL,
                  @CARRIER_CD VARCHAR(29) = NULL,
                  @CARRIER_NO VARCHAR(29) = NULL;

            select @uat_number = a.RECPTCL_FID,
                   @expedition_date = g.ROUTE_DEPARTURE_DT,
                   @consignment = x.CONSGNT_FID,
                   @flight = substring(h.DEST_LOCATION_FCD,3,3),
                   @arrival_date = h.ARRIVAL_DT,
                   @CARRIER_CD = h.CARRIER_CD,
                   @CARRIER_NO = h.CARRIER_NO,
                   @MAIL_CLASS_NM = f.MAIL_CLASS_CD,
                   @MAIL_SUBCLASS_NM = a.mail_subclass_fcd
              from l_recptcls a with (nolock)
                   inner join l_recptcl_events b with (nolock) on a.RECPTCL_PID = b.RECPTCL_PID and b.EVENT_TYPE_CD = 101
                   inner join L_CONSGNTS x with (nolock) on a.evt_consgnt_pid = x.CONSGNT_PID
                   inner join L_CONSGNT_EVENTS c with (nolock) on x.consgnt_pid = c.CONSGNT_PID and c.EVENT_TYPE_CD = 301
                   left join L_USERS z with (nolock) on c.USER_PID = z.USER_PID
                   inner join l_mailitms d with (nolock) on d.EVT_RECPTCL_PID = a.RECPTCL_PID
                   inner join C_MAIL_SUBCLASSES e with (nolock) on e.mail_subclass_fcd = a.mail_subclass_fcd
                   inner join C_MAIL_CLASSES f with (nolock) on f.mail_class_cd = e.mail_class_cd
                   left join l_routes g with (nolock) on g.ROUTE_PID = x.ROUTE_PID
                   left join l_legs h with (nolock) on h.LEG_PID = g.CN_LEG_PID
             where substring(a.RECPTCL_fid, 1, 2) = 'PT'
               and substring(a.RECPTCL_fid, 7, 2) <> 'PT'
               and substring(a.RECPTCL_fid, 14,2) <> 'TT'
               and c.EVENT_GMT_DT between @DT_FROM  and @DT_TO
               and d.MAILITM_FID = ISNULL(@object_num,d.MAILITM_FID)
            ORDER BY g.ROUTE_DEPARTURE_DT, a.RECPTCL_FID, h.ARRIVAL_DT;

            SELECT @uat_number AS UAT,
                   @MAIL_CLASS_NM AS MC,
                   @MAIL_SUBCLASS_NM AS MSC,
                   @DT_FROM AS DT_FROM,
                   @DT_TO AS DT_TO;
        """

        cursor.execute(select_query, tracking_number)

        # pegar **último result set** (o SELECT final)
        base_info = fetch_last_resultset_row(cursor)

        if not base_info or base_info[0] is None:
            return None

        uat_number = base_info[0]
        mail_class = base_info[1]
        mail_subclass = base_info[2]
        dt_from = base_info[3]
        dt_to = base_info[4]

        # -----------------------------------------------------
        # 2) SP FINAL
        # -----------------------------------------------------
        cursor.execute(
            """
            EXEC dbo.CTT_SP_REL_CONTROLO_TRANSPORTE_INT
                @DT_FROM = ?,
                @DT_TO = ?,
                @recptcl_fid = ?,
                @tipo_mail = ?,
                @tipo_subclasse = ?;
            """,
            (dt_from, dt_to, uat_number, mail_class, mail_subclass),
        )

        sp_result = fetch_last_resultset_row(cursor)

        if not sp_result:
            return None

        # -----------------------------------------------------
        # 3) MAPEAR
        # -----------------------------------------------------
        return parse_result(sp_result)

    except pyodbc.Error as e:
        print(f"Erro ao executar SQL: {e}")
        return None
