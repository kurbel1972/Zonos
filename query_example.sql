DECLARE
      @DT_FROM DATETIME = DATEADD(DAY, -30, GETDATE()),
      @DT_TO   DATETIME = DATEADD(DAY, +10, GETDATE()),
      @object_num VARCHAR(29) = 'LA131568770PT',  
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

EXEC dbo.CTT_SP_REL_CONTROLO_TRANSPORTE_INT
        @DT_FROM       = @DT_FROM,
        @DT_TO         = @DT_TO,
        @recptcl_fid   = @uat_number,
        @tipo_mail     = @MAIL_CLASS_NM,
        @tipo_subclasse = @MAIL_SUBCLASS_NM;
GO

