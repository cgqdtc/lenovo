CREATE OR REPLACE PACKAGE BODY "PKG_PCDW_TO_MRP" AS
  /******************************************************************************
   NAME: PRDSPOEMTHK.PKG_PCDW_TO_MRP
   PURPOSE:
   REVISIONS:
   Ver       Date       Author          Description
   --------- ---------- --------------- ------------------------------------
   1.0       2017-7-17  GLP
   2.0       2017-7-18  zhangyu3        add fru PO-
  ******************************************************************************/

  PROCEDURE prc_odm_inventory(wfl_id VARCHAR2, node_id VARCHAR2, iv_id VARCHAR2, exitcode OUT NUMBER) AS
    l_proc_name VARCHAR2(40) := g_logic_name || '.PRC_ODM_INVENTORY';
  BEGIN
    exitcode         := -20099;
    logger.wfl_id    := wfl_id;
    logger.node_id   := node_id;
    logger.log_level := 'ALL';

    pkg_base_funcs.init_log_run(g_logic_name || ':' || iv_id);

    logger.info(l_proc_name || ' Start');

    EXECUTE IMMEDIATE 'TRUNCATE table z_pcdw_odm_inventory';


      /* *************************************************************************************
    Backgroud : SA&V project partitial done , Bitland will use SAV data source--- added by yangnan13 20240819
    ****************************************************************************************/
    --step 1 SCC source added by yangnan13 for Bitland 20240819
    --confirmed with jiaqi, DT no need match lgort table
    EXECUTE IMMEDIATE 'TRUNCATE table z_mid_source_odminv';
      insert into  z_mid_source_odminv
    (FLAG, STATUS, ODM, LENOVOPN, ODMPN, INV_QTY, EFFSTARTDATE, CTO_PO, BU, ODM_CREATED_DATE, SYS_CREATED_BY, SYS_CREATED_DATE, BQTY)
      select /*+ use_hash(a b)  driving_site(b) */ 'IN-ODM' as FLAG, 'ACTIVE' as STATUS, A.ODM,A.LENOVO_PN,A.ODM_PN,sum(A.ODM_QTY)as odm_qty,max(A.CREATE_TIME) AS EFFSTARTDATE,A.PO,A.BU,
    MAX(A.CREATE_TIME) AS ODM_CREATED_DATE,
    'SCC',SYSDATE, null as BQTY FROM I_SCC_INTEGRATION.ODM_INVENTORY@PRDCSEN  A
    --left join I_SCC_INTEGRATION.ODM_LOCATION_MAPPING@PRDCSEN  B
    --on a.location = b.odm_location
    where a.version=to_char(sysdate,'YYYYMMDD')
    AND a.BU<>'ISG'
    and a.bu not like'Mobile%'
   --and lower(b.status) = 'active'
  --and lower(b.LOCATION_TYPE) = 'unrestricted'
  --and b.flag = 'IN-ODM'
  and exists ( select 1 from (SELECT distinct  INV_SOURCE, PVALUE
                                    FROM z_ui_conf_parameter
                                   WHERE pdomain = 'ODM_CODE' ) c
                        where UPPER(a.odm) = c.pvalue
                        and c. INV_SOURCE = 'SCC')
    group by a.ODM,a.LENOVO_PN,a.ODM_PN,a.PO,a.BU,a.version;
    commit;
    -- added by yangnan13 for Bitland 20240819
       INSERT INTO z_pcdw_odm_inventory
      (odm, lenovopn, odmpn, inv_qty, effstartdate, cto_po, bu, odm_created_date, sys_created_by,
       sys_created_date)
      SELECT DECODE(upper(odm),'FOXCONNL6','FOXCONN',odm), nvl(lenovopn, odmpn), odmpn, inv_qty, effstartdate, cto_po, func_odmpn_format(bu),
             odm_created_date, 'PKG_PCDW_TO_MRP', SYSDATE
        FROM  z_mid_source_odminv a
        where upper(a.ODM) in ('BITLAND','DIXON');
        commit;

    INSERT INTO z_pcdw_odm_inventory
      (odm, lenovopn, odmpn, inv_qty, effstartdate, cto_po, bu, odm_created_date, sys_created_by,
       sys_created_date)
      SELECT DECODE(upper(odm),'FOXCONNL6','FOXCONN',odm), nvl(lenovopn, odmpn), odmpn, inv_qty, effstartdate, cto_po, func_odmpn_format(bu),
             odm_created_date, 'PKG_PCDW_TO_MRP', SYSDATE
        FROM (SELECT * FROM prdpcdw.imp_odm_inventory where upper(ODM) <> 'BITLAND');-- added by yangnan13 for bitland 20240902

    COMMIT;

    --BU
    /*UPDATE z_pcdw_odm_inventory
       SET bu = decode(odm, 'QUANTA', decode(cto_po, 'LBG', 'IDEANB', 'TBG', 'THINKNB', bu), 'LCFC',
                       decode(bu, 'T-NB', 'THINKNB','T-WS', 'WorkStation', 'L-DT', 'IDEADT', 'T-SSD', NULL, 'L-NB', 'IDEANB', 'T-SV',
                               NULL, 'SD', NULL, bu), bu)
     WHERE odm IN ('LCFC', 'QUANTA');
    COMMIT;*/
    --edit by xuml7 20210629 for remove lcfc useless inv
    UPDATE z_pcdw_odm_inventory
       SET bu = decode(odm, 'QUANTA', decode(cto_po, 'LBG', 'IDEANB', 'TBG', 'THINKNB', bu), 'LCFC',
                       decode(bu, 'T-NB', 'THINKNB', 'T-DT', 'THINKDT','L-DT', 'IDEADT', 'L-NB', 'IDEANB',
                              'T-WS', 'WorkStation', 'T-SSD', 'NOUSE', 'T-SV', 'NOUSE', 'SD', 'NOUSE',
                              'JV-EC', 'NOUSE','JV-NBI', 'NOUSE','JV-SVC', 'NOUSE', bu), bu)
     WHERE odm IN ('LCFC', 'QUANTA');
    COMMIT;

    UPDATE z_pcdw_odm_inventory a
       SET lenovopn = substr(a.odmpn, 0, 7)
     WHERE a.odm IN ('WISTRON', 'WISTRON ZS','WISTRON_VN','WISTRON_VN_NB')-- added by yangnan13 20250103 for VN
       AND EXISTS (SELECT 1 FROM z_dim_component_bs b WHERE substr(a.odmpn, 0, 7) = b.component)
       AND NOT EXISTS (SELECT 1 FROM z_dim_component_bs c WHERE a.lenovopn = c.component);

    COMMIT;

    UPDATE z_pcdw_odm_inventory a
       SET lenovopn = substr(a.odmpn, 0, 10)
     WHERE a.odm IN ('WISTRON', 'WISTRON ZS','WISTRON_VN','WISTRON_VN_NB')-- added by yangnan13 20250103 for VN
       AND EXISTS (SELECT 1 FROM z_dim_component_bs b WHERE substr(a.odmpn, 0, 10) = b.component)
       AND NOT EXISTS (SELECT 1 FROM z_dim_component_bs c WHERE a.lenovopn = c.component);

    COMMIT;
    update z_pcdw_odm_inventory
    set lenovopn=REPLACE(lenovopn, 'ForLCFC', '')
    where odm='BITLAND' and instr(lenovopn,'ForLCFC')>0;
    commit;--add by xurt1 for error PN in BITLAND(SSA0X01084for LCFC)

    --add by xuml7 for compal inv which have '/'
    update Z_PCDW_ODM_INVENTORY



    set lenovopn = regexp_substr(LENOVOPN,'[^/]+', 1, 1)
    where lenovopn like '%/%' and odm = 'COMPAL';
    commit;
    --end add 20210224



    IF to_number(to_char(SYSDATE, 'D', 'NLS_DATE_LANGUAGE = American')) = v_snapdate
    THEN
     EXECUTE IMMEDIATE 'ALTER TABLE Z_PCDW_ODM_INVENTORY_HIS TRUNCATE SUBPARTITION P_' ||
                        to_char(v_currentweek, 'YYYY') || '_W_' || to_char(v_currentweek, 'WW');

      INSERT INTO z_pcdw_odm_inventory_his
        (versionyear, versionweek, versiondate, odm, lenovopn, odmpn, inv_qty, effstartdate, cto_po, bu,
         odm_created_date, sys_created_by, sys_created_date)
        SELECT trunc(v_currentweek, 'yyyy'), to_char(v_currentweek, 'ww'), v_currentweek, odm,
               pkg_base_funcs.get_matnr_input(lenovopn), pkg_base_funcs.get_matnr_input(odmpn), inv_qty,
               effstartdate, cto_po, bu, odm_created_date, sys_created_by, sys_created_date
          FROM z_pcdw_odm_inventory;

      COMMIT;

    --UPDATE FOR MATCH CODE  BY ZHAIBO 20180112
    UPDATE Z_PCDW_ODM_INVENTORY_HIS
       SET bu = replace(cto_po,chr(13),'')
     WHERE odm = 'QUANTA'
       AND versiondate = v_currentweek;

    COMMIT;

    ---bu for WISTRON COMP, eg lenovo item is ***AA, or 0000+ odm item
    UPDATE Z_PCDW_ODM_INVENTORY_HIS
       SET BU = 'THINKNB'
     WHERE versiondate = v_currentweek
       and ODM = 'WISTRON'
       and PKG_BASE_FUNCS.get_matnr_input(ODMPN) <> PKG_BASE_FUNCS.get_matnr_input(LENOVOPN) --ODMPN <> LENOVOPN
       and LENOVOPN IS NOT NULL;
       --AND INSTR(ODMPN, LENOVOPN) > 0
       --AND LENGTH(ODMPN) > LENGTH(LENOVOPN);

    COMMIT;

    --- bu for WISTRON COMP
    UPDATE Z_PCDW_ODM_INVENTORY_HIS a
       set BU = 'IDEANB'
     where ODM = 'WISTRON'
       AND versiondate = v_currentweek
       AND PKG_BASE_FUNCS.get_matnr_input(ODMPN) = PKG_BASE_FUNCS.get_matnr_input(LENOVOPN)         -- ODMPN = LENOVOPN
       AND exists ( select 1 from z_dim_component b where a.LENOVOPN = b.component); --add filter to filter out odm sfg part number 20180131


    COMMIT;


    --DETERMINE bu for WISTRON MTM
    UPDATE Z_PCDW_ODM_INVENTORY_HIS A
       SET BU =
           (SELECT HIER6_CODE
              FROM Z_DIM_PRODUCT_BS B
             WHERE A.LENOVOPN = B.PRODUCT)
     WHERE EXISTS (SELECT 1 FROM Z_DIM_PRODUCT_BS C WHERE A.LENOVOPN = C.PRODUCT)
       AND ODM = 'WISTRON'
       AND VERSIONDATE = v_currentweek;

    COMMIT;

    ---bu for WISTRON OPTION
    /*UPDATE Z_PCDW_ODM_INVENTORY_HIS
       SET BU = 'THINKNB'
     WHERE versiondate = v_currentweek
       and ODM = 'WISTRON'
       AND BU = 'OPTION';

    COMMIT;*/
    --edit by xuml7 20231115 for wangqun10&zhaojq7
    UPDATE Z_PCDW_ODM_INVENTORY_HIS A
       SET BU =
           (SELECT CASE WHEN B.BRAND = 'IDEAOPTION' THEN 'IDEANB' ELSE 'THINKNB' END
              FROM PRDPCDW.Z_DSP_DIM_PRODUCT B
             WHERE A.LENOVOPN = B.PRODUCT)
     WHERE EXISTS (SELECT 1 FROM PRDPCDW.Z_DSP_DIM_PRODUCT C WHERE A.LENOVOPN = C.PRODUCT and C.HIER6_CODE = 'OPTION')
       AND ODM = 'WISTRON'
       AND VERSIONDATE = v_currentweek;
     COMMIT;

    ---bu for Wistron SFG
    UPDATE Z_PCDW_ODM_INVENTORY_HIS A
       SET BU =
           (SELECT BU
              FROM z_mid_mat_util_comp_bu_family B
             WHERE A.ODMPN = B.COMP
               AND A.ODM = B.WERKS
               and rownum = 1)
     WHERE EXISTS (SELECT 1
              FROM z_mid_mat_util_comp_bu_family C
             WHERE A.ODMPN = C.COMP
               AND A.ODM = C.WERKS)
       AND ODM = 'WISTRON'
       AND versiondate = v_currentweek
       --AND LENOVOPN IS NULL
       AND BU IS NULL;

    COMMIT;
    ---END CHANGE

    END IF;


    logger.info(l_proc_name || ' Completed');

    exitcode := 0;
  EXCEPTION
    WHEN OTHERS THEN
      exitcode := SQLCODE;
      logger.error;
      RAISE;
  END;

  PROCEDURE prc_odm_sfg_shipmen(wfl_id VARCHAR2, node_id VARCHAR2, iv_id VARCHAR2, exitcode OUT NUMBER) AS
    l_proc_name VARCHAR2(40) := g_logic_name || '.PRC_ODM_SFG_SHIPMEN';
  BEGIN
    exitcode         := -20099;
    logger.wfl_id    := wfl_id;
    logger.node_id   := node_id;
    logger.log_level := 'ALL';

    pkg_base_funcs.init_log_run(g_logic_name || ':' || iv_id);

    logger.info(l_proc_name || ' Start');

    EXECUTE IMMEDIATE 'truncate table z_pcdw_odm_sfg_shipment';

    INSERT INTO z_pcdw_odm_sfg_shipment
      (odm, odmpn, lenovopn, ship_qty, ship_date, ship_to, odm_create_date, sys_created_by, sys_created_date)
      SELECT DECODE(upper(odm),'FOXCONNL6','FOXCONN',odm), odmpn, nvl(lenovopn, odmpn), SUM(ship_qty), ship_date, ship_to, max(odm_create_date) as odm_create_date,
             g_logic_name, SYSDATE
        FROM prdpcdw.imp_odm_sfg_shipment a
        where length(nvl(lenovopn, odmpn)) <= 30
       /*
       WHERE a.rowid = (SELECT MIN(b.rowid)
                          FROM prdpcdw.imp_odm_sfg_shipment b
                         WHERE upper(a.odm) = upper(b.odm)
                           AND a.odmpn = b.odmpn
                           AND nvl(a.lenovopn, a.odmpn) = nvl(b.lenovopn, b.odmpn)
                           AND a.ship_date = b.ship_date
                           AND a.ship_to = b.ship_to)
        */
       GROUP BY DECODE(upper(odm),'FOXCONNL6','FOXCONN',odm), odmpn, nvl(lenovopn, odmpn), ship_date, ship_to--, odm_create_date
       ;

    COMMIT;

    logger.info(l_proc_name || ' Completed');

    --update quanta
    update z_pcdw_odm_sfg_shipment a
    set ship_to = 'SH_NB'
    where odm = 'QUANTA' and lower(func_odmpn_format(ship_to) ) = 'shanghai';

    commit;
    --update liteon
    update z_pcdw_odm_sfg_shipment a
    set ship_to = (select max(b.ship_to)
               from (SELECT distinct pattribute as ship_to
                     FROM z_ui_conf_parameter
                    WHERE pdomain = 'INTRANSIT'
                      AND pname = 'DESTINATION') b
               where instr(func_odmpn_format(a.ship_to),b.ship_to) >0
               )
    where odm = 'LITEON'
    and exists (select 1
               from (SELECT distinct pattribute as ship_to
                     FROM z_ui_conf_parameter
                    WHERE pdomain = 'INTRANSIT'
                      AND pname = 'DESTINATION') b
               where instr(func_odmpn_format(a.ship_to),b.ship_to) >0
               );

    commit;

    update z_pcdw_odm_sfg_shipment a
    set ship_to = 'FLEX_DT'
    where odm = 'LITEON' and upper(func_odmpn_format(ship_to) ) like 'FLEX%';

    commit;
    --foxconn need update, but cannot split nb/dt

    ---sfg gr net change
    pkg_odm_supply. prc_odm_sfg_intransit_gr(wfl_id,node_id,iv_id,exitcode);

    exitcode := 0;
  EXCEPTION
    WHEN OTHERS THEN
      exitcode := SQLCODE;
      logger.error;
      RAISE;
  END;

  PROCEDURE prc_odm_sfg_intrans(wfl_id VARCHAR2, node_id VARCHAR2, iv_id VARCHAR2, exitcode OUT NUMBER) AS
    l_proc_name VARCHAR2(40) := g_logic_name || '.PRC_ODM_SFG_INTRANS';

  BEGIN
    exitcode         := -20099;
    logger.wfl_id    := wfl_id;
    logger.node_id   := node_id;
    logger.log_level := 'ALL';

    pkg_base_funcs.init_log_run(g_logic_name || ':' || iv_id);

    logger.info(l_proc_name || ' Start');

    EXECUTE IMMEDIATE 'truncate table z_pcdw_odm_sfg_intransit';
     EXECUTE IMMEDIATE 'truncate table Z_TAXPN_MAP_COMPAL';

    INSERT INTO z_pcdw_odm_sfg_intransit
      (odm, odmpn, lenovopn, ship_qty, ship_date, ship_to, odm_create_date, sys_created_by, sys_created_date)
      SELECT DECODE(upper(odm),'FOXCONNL6','FOXCONN',odm), nvl(odmpn, lenovopn), nvl(lenovopn, odmpn), SUM(ship_qty), ship_date, ship_to,
             MAX(odm_create_date), g_logic_name, SYSDATE
        FROM prdpcdw.imp_odm_sfg_intransit
       GROUP BY odm, nvl(odmpn, lenovopn), nvl(lenovopn, odmpn), ship_date, ship_to;

    COMMIT;

INSERT INTO z_pcdw_odm_sfg_intransit --add wanggang38 2024-12-20 FOR_VN except COMPAL_VN-->COMPAL situation intransit
  (odm,
   odmpn,
   lenovopn,
   ship_qty,
   ship_date,
   ship_to,
   odm_create_date,
   sys_created_by,
   sys_created_date)
  select ODM,
         ODMPN,
         LENOVOPN,
         SHIP_QTY,
         SHIP_DATE,
         SHIP_TO,
         ODM_CREATE_DATE,
         L_PROC_NAME,
         SYSDATE
    from (select ODM,
                 ODMPN,
                 LENOVOPN,
                 SHIP_QTY,
                 SHIP_DATE,
                 SHIP_TO,
                 ODM_CREATE_DATE,
                 row_number() over(partition by ODM, ODMPN, LENOVOPN, SHIP_QTY, SHIP_DATE, SHIP_TO order by ODM, ODMPN, LENOVOPN, SHIP_QTY, SHIP_DATE, SHIP_TO, VERSION desc) rn
            from (SELECT UPPER(ODM) /*|| '_VN'*/ ODM,
                         NVL(ODM_PN, LENOVO_PN) odmpn,
                         nvl(LENOVO_PN, ODM_PN) lenovopn,
                         sum(DELIVERY_QTY) ship_qty,
                         SHIP_DATE,
                         UPPER(odm) ship_to,
                         MAX(CREATE_TIME) odm_create_date,
                         VERSION
                    FROM I_SCC_INTEGRATION.ODM_ASN@PRDCSEN a
                   where  exists 
                   (select 1 from (select ODM,max(VERSION) version from  I_SCC_INTEGRATION.ODM_ASN@PRDCSEN a group by ODM) b
                   where a.version=b.version and a.odm=b.odm)      -- added by yangnan13 20250123 for each ODM get each max version                    
                  and UPPER(STATUS) = 'OPEN'
                  and PROCUREMENT_MODE in ('ODM Internal Rebalance')
                  and UPPER(odm) in ('WISTRON','LCFC','HUAQIN','LCFC_VN','HUAQIN_VN','COMPAL_VN','WISTRON_VN_NB')
                  and upper(BU) like '%NB%'
                   group by UPPER(ODM),
                            NVL(ODM_PN, LENOVO_PN),
                            nvl(LENOVO_PN, ODM_PN),
                            SHIP_DATE,
                            VERSION))
   where rn = 1;
   COMMIT;

-- COMPAL_VN special deal with --> intransit start by 72A1%, should transilate to 451AX to  z_pcdw_odm_sfg_intransit
--step 1 get mapping relationship
   insert into Z_TAXPN_MAP_COMPAL
   (ODMPN_INSTRANSIT, ODM_PN, packing_pn, odm, ODM_CREATED_DATE)
   select ODMPN_INSTRANSIT, ODM_PN, LENOVO_PN, FROM_ODM, ODM_CREATED_DATE
     from (select ODMPN_INSTRANSIT,
                  ODM_PN,
                  LENOVO_PN,
                  FROM_ODM,
                  ODM_CREATED_DATE,
                  version,
                  row_number() over(partition by ODMPN_INSTRANSIT, ODM_PN, LENOVO_PN, FROM_ODM, ODM_CREATED_DATE order by ODMPN_INSTRANSIT, ODM_PN, LENOVO_PN, FROM_ODM, ODM_CREATED_DATE, VERSION desc) rn
             from (select A.ODMPN_INSTRANSIT,
                          A.ODM_PN,
                          a.LENOVO_PN,
                          A.from_odm,
                          max(A.ODM_CREATED_DATE) as ODM_CREATED_DATE,
                          version
                     from I_SCC_INTEGRATION.odm_imp_source@PRDCSEN A
                    WHERE ODMPN_INSTRANSIT IS NOT NULL
                      and upper(from_odm) = 'COMPAL_VN'
                      and  exists (select 1 from (select from_odm,max(VERSION) version from  I_SCC_INTEGRATION.odm_imp_source@prdcsen a group by from_odm) b
                            where a.version=b.version and a.from_odm=b.from_odm)
                    group by A.ODMPN_INSTRANSIT,
                             A.ODM_PN,
                             a.LENOVO_PN,
                             A.from_odm,
                             version))
    where rn = 1;
commit;

--step 2 insert into stock for compal_vn--> compal
INSERT INTO z_pcdw_odm_sfg_intransit --add wanggang38 2024-12-20 FOR_VN intransit
  (odm,
   odmpn,
   lenovopn,
   ship_qty,
   ship_date,
   ship_to,
   odm_create_date,
   sys_created_by,
   sys_created_date)
  select ODM,
         ODM_PN,
         LENOVO_PN,
         SHIP_QTY,
         SHIP_DATE,
         SHIP_TO,
         ODM_CREATE_DATE,
         L_PROC_NAME,
         SYSDATE
    from (select ODM,
                 ODM_PN,
                 LENOVO_PN,
                 SHIP_QTY,
                 SHIP_DATE,
                 SHIP_TO,
                 ODM_CREATE_DATE,
                 version,
                 row_number() over(partition by ODM, ODM_PN, ODM_PN, SHIP_QTY, SHIP_DATE, SHIP_TO, VERSION order by ODM, ODM_PN, ODM_PN, SHIP_QTY, SHIP_DATE, SHIP_TO, VERSION desc) rn
            from (SELECT UPPER(a.ODM) /*|| '_VN'*/ ODM,
                         a.ODM_PN AS  ODM_PN,
                         B.PACKING_PN as   LENOVO_PN,
                         sum(DELIVERY_QTY) ship_qty,
                         SHIP_DATE,
                         a.odm AS ship_to,
                         MAX(CREATE_TIME) odm_create_date,
                         version
                    FROM ( select *
                            from I_SCC_INTEGRATION.ODM_ASN@PRDCSEN a
                           where  exists (select 1 from (select ODM,max(VERSION) version from I_SCC_INTEGRATION.ODM_ASN@PRDCSEN a group by ODM) b
                   where a.version=b.version and a.odm=b.odm) ) a
                   inner join Z_TAXPN_MAP_COMPAL b
                      on a.ODM_PN = b.ODMPN_INSTRANSIT
                   WHERE UPPER(STATUS) = 'OPEN'
                     and PROCUREMENT_MODE in ('ODM Internal Rebalance')
                     and UPPER(a.odm) in ('COMPAL')
                     and upper(BU) like '%NB%'
                   group by a.ODM, b.PACKING_PN, a.ODM_PN, ship_date, version))
   where rn = 1;
commit;

    UPDATE z_pcdw_odm_sfg_intransit a
       SET lenovopn = substr(a.odmpn, 0, 10)
     WHERE a.odm IN ('WISTRON', 'WISTRON ZS','WISTRON_VN','WISTRON_VN_NB')-- added by yangnan13 20250103 for VN
       AND EXISTS (SELECT 1 FROM z_dim_component_bs b WHERE substr(a.odmpn, 0, 10) = b.component)
       AND NOT EXISTS (SELECT 1 FROM z_dim_component_bs c WHERE a.lenovopn = c.component);
    COMMIT;
    
    update z_pcdw_odm_sfg_intransit a
    set lenovopn = null
    where odm in('HUAQIN','COMPAL','COMPAL_VN', 'WISTRON','WISTRON_VN_NB') and nvl(lenovopn,' ') in('NA','N/A');
    COMMIT;
    
    IF to_number(to_char(SYSDATE, 'D', 'NLS_DATE_LANGUAGE = American')) = v_snapdate
    THEN
      EXECUTE IMMEDIATE 'ALTER TABLE Z_PCDW_ODM_SFG_INTRANSIT_HIS TRUNCATE SUBPARTITION P_' ||
                        to_char(v_currentweek, 'YYYY') || '_W_' || to_char(v_currentweek, 'WW');

      INSERT INTO z_pcdw_odm_sfg_intransit_his
        (versionyear, versionweek, versiondate, odm, odmpn, lenovopn, ship_qty, ship_date, ship_to,
         odm_create_date, sys_created_by, sys_created_date)
        SELECT trunc(v_currentweek, 'yyyy'), to_char(v_currentweek, 'ww'), v_currentweek, odm, odmpn, lenovopn,
               ship_qty, ship_date, nvl(ship_to, ' '), odm_create_date, sys_created_by, sys_created_date
          FROM z_pcdw_odm_sfg_intransit;

      COMMIT;

    END IF;

    logger.info(l_proc_name || ' Completed');

    exitcode := 0;
  EXCEPTION
    WHEN OTHERS THEN
      exitcode := SQLCODE;
      logger.error;
      RAISE;
  END;

  PROCEDURE prc_odm_wip(wfl_id VARCHAR2, node_id VARCHAR2, iv_id VARCHAR2, exitcode OUT NUMBER) AS
    l_proc_name VARCHAR2(40) := g_logic_name || '.PRC_ODM_WIP';
  BEGIN
    exitcode         := -20099;
    logger.wfl_id    := wfl_id;
    logger.node_id   := node_id;
    logger.log_level := 'ALL';

    pkg_base_funcs.init_log_run(g_logic_name || ':' || iv_id);

    logger.info(l_proc_name || ' Start');

    EXECUTE IMMEDIATE 'truncate table z_pcdw_odm_wip';

    INSERT INTO z_pcdw_odm_wip
      (odm, lenovopn, odmpn, wip_qty, odm_created_date, sys_created_by, sys_created_date, bu)
      SELECT DECODE(upper(odm),'FOXCONNL6','FOXCONN',odm), lenovopn, odmpn, wip_qty, odm_created_date, g_logic_name, SYSDATE,
             replace(bu, chr(13),'')--DECODE(upper(odm),'3NOD',PO,'')
        FROM prdpcdw.imp_odm_wip;

    COMMIT;

    UPDATE z_pcdw_odm_wip a
       SET lenovopn = substr(a.odmpn, 0, 7)
     WHERE a.odm IN ('WISTRON', 'WISTRON ZS','WISTRON_VN_NB')-- added 'WISTRON_VN_NB' by yangnan13 20250120
       AND EXISTS (SELECT 1 FROM z_dim_component_bs b WHERE substr(a.odmpn, 0, 7) = b.component)
       AND NOT EXISTS (SELECT 1 FROM z_dim_component_bs c WHERE a.lenovopn = c.component);

    COMMIT;

    UPDATE z_pcdw_odm_wip a
       SET lenovopn = substr(a.odmpn, 0, 10)
     WHERE a.odm IN ('WISTRON', 'WISTRON ZS','WISTRON_VN_NB')-- added 'WISTRON_VN_NB' by yangnan13 20250120
       AND EXISTS (SELECT 1 FROM z_dim_component_bs b WHERE substr(a.odmpn, 0, 10) = b.component)
       AND NOT EXISTS (SELECT 1 FROM z_dim_component_bs c WHERE a.lenovopn = c.component);

    COMMIT;

    IF to_number(to_char(SYSDATE, 'D', 'NLS_DATE_LANGUAGE = American')) = v_snapdate
    THEN
      EXECUTE IMMEDIATE 'ALTER TABLE Z_PCDW_ODM_WIP_HIS TRUNCATE SUBPARTITION P_' ||
                        to_char(v_currentweek, 'YYYY') || '_W_' || to_char(v_currentweek, 'WW');

      INSERT INTO z_pcdw_odm_wip_his
        (versionyear, versionweek, versiondate, odm, lenovopn, odmpn, wip_qty, odm_created_date,
         sys_created_by, sys_created_date, bu)
        SELECT trunc(v_currentweek, 'yyyy'), to_char(v_currentweek, 'ww'), v_currentweek, odm,
               pkg_base_funcs.get_matnr_input(lenovopn), pkg_base_funcs.get_matnr_input(odmpn), wip_qty,
               odm_created_date, sys_created_by, sys_created_date, bu
          FROM z_pcdw_odm_wip;

      COMMIT;

    --- update bu for Quanta/Wistron, item souce to one BU; item mapping to multiple BU will source to idea
    MERGE INTO z_pcdw_odm_wip_his t1
    USING (SELECT item, siteid, COUNT(1) cnt, MAX(bu) bu, decode(COUNT(1), 1, MAX(bu), 'IdeaNB') AS bu_f
             FROM (SELECT DISTINCT comp item, werks siteid, bu
                      FROM z_mid_mat_util_comp_bu_family
                     WHERE werks in ('WISTRON','QUANTA'))
            GROUP BY item, siteid) t2
    ON (t1.lenovopn = t2.item AND t1.ODM = t2.siteid)
    WHEN MATCHED THEN
      UPDATE SET t1.bu = t2.bu_f WHERE t1.versiondate = v_currentweek;

    COMMIT;

    --DETERMINE bu for Quanta/WISTRON MTM
    UPDATE z_pcdw_odm_wip_his A
       SET BU =
           (SELECT HIER6_CODE
              FROM Z_DIM_PRODUCT_BS B
             WHERE A.LENOVOPN = B.PRODUCT)
     WHERE EXISTS (SELECT 1 FROM Z_DIM_PRODUCT_BS C WHERE A.LENOVOPN = C.PRODUCT)
       AND ODM in ('WISTRON','QUANTA')
       AND VERSIONDATE = v_currentweek;

    COMMIT;

    END IF;

    logger.info(l_proc_name || ' Completed');

    exitcode := 0;
  EXCEPTION
    WHEN OTHERS THEN
      exitcode := SQLCODE;
      logger.error;
      RAISE;
  END;

  PROCEDURE prc_odm_mo_reservat(wfl_id VARCHAR2, node_id VARCHAR2, iv_id VARCHAR2, exitcode OUT NUMBER) AS
    l_proc_name VARCHAR2(40) := g_logic_name || '.PRC_ODM_MO_RESERVAT';
  BEGIN
    exitcode         := -20099;
    logger.wfl_id    := wfl_id;
    logger.node_id   := node_id;
    logger.log_level := 'ALL';

    pkg_base_funcs.init_log_run(g_logic_name || ':' || iv_id);

    logger.info(l_proc_name || ' Start');

    EXECUTE IMMEDIATE 'truncate table z_pcdw_odm_mo_reservation';

    INSERT INTO z_pcdw_odm_mo_reservation
      (odm, odm_item, lenovo_item, res_qty, res_date, sys_created_by, sys_created_date, bu)
      SELECT DECODE(upper(odm),'FOXCONNL6','FOXCONN',odm), odm_item,
             CASE
               WHEN TRIM(lenovo_item) IS NULL THEN
                odm_item
               ELSE
                pkg_base_funcs.get_matnr_input(REPLACE(lenovo_item, '-', ''))
             END AS lenovo_item, res_qty, res_date, g_logic_name, SYSDATE, ''
        FROM prdpcdw.imp_odm_mo_reservation
        WHERE ODM NOT IN('LITEON','FOXCONNL6'); --Modify by hanz7 20220616 Old value: ODM<>'LITEON'


    INSERT INTO z_pcdw_odm_mo_reservation
      (odm, odm_item, lenovo_item, res_qty, res_date, sys_created_by, sys_created_date, bu)
      SELECT DECODE(upper(odm),'FOXCONNL6','FOXCONN',odm), odm_item,
             CASE
               WHEN TRIM(lenovo_item) IS NULL THEN
                odm_item
               ELSE
                pkg_base_funcs.get_matnr_input(REPLACE(lenovo_item, '-', ''))
             END AS lenovo_item, res_qty, res_date, g_logic_name, SYSDATE,
            -- decode(REPLACE(REPLACE(bu, CHR(10)), CHR(13)),'TBG','ThinkDT','LBG','IdeaDT',bu)
             decode(replace(replace(bu,chr(13),''),chr(10),''),'TBG','ThinkDT','LBG','IdeaDT',replace(replace(bu,chr(13),''),chr(10),''))--add wanggang38 2023-7-7
        FROM prdpcdw.imp_odm_mo_reservation
        WHERE ODM IN('LITEON','FOXCONNL6'); --Modify by hanzx7 20220616  Old value: ODM='LITEON'
    COMMIT;

    UPDATE z_pcdw_odm_mo_reservation a
       SET lenovo_item = substr(a.odm_item, 0, 7)
     WHERE a.odm IN ('WISTRON', 'WISTRON ZS')
       AND EXISTS (SELECT 1 FROM z_dim_component_bs b WHERE substr(a.odm_item, 0, 7) = b.component)
       AND NOT EXISTS (SELECT 1 FROM z_dim_component_bs c WHERE a.lenovo_item = c.component);

    COMMIT;

    UPDATE z_pcdw_odm_mo_reservation a
       SET lenovo_item = substr(a.odm_item, 0, 10)
     WHERE a.odm IN ('WISTRON', 'WISTRON ZS')
       AND EXISTS (SELECT 1 FROM z_dim_component_bs b WHERE substr(a.odm_item, 0, 10) = b.component)
       AND NOT EXISTS (SELECT 1 FROM z_dim_component_bs c WHERE a.lenovo_item = c.component);

    COMMIT;

    MERGE INTO z_pcdw_odm_mo_reservation a
    USING z_dim_product_bs b
    ON (a.lenovo_item = b.product)
    WHEN MATCHED THEN
      UPDATE SET bu = b.hier6_code;

    COMMIT;

    IF to_number(to_char(SYSDATE, 'D', 'NLS_DATE_LANGUAGE = American')) = v_snapdate
    THEN
      EXECUTE IMMEDIATE 'ALTER TABLE Z_PCDW_ODM_MO_RESERVATION_HIS TRUNCATE SUBPARTITION P_' ||
                        to_char(v_currentweek, 'YYYY') || '_W_' || to_char(v_currentweek, 'WW');

      INSERT INTO z_pcdw_odm_mo_reservation_his
        (versionyear, versionweek, versiondate, odm, odm_item, lenovo_item, res_qty, res_date, sys_created_by,
         sys_created_date, bu)
        SELECT trunc(v_currentweek, 'yyyy'), to_char(v_currentweek, 'ww'), v_currentweek, odm, odm_item,
               lenovo_item, res_qty, res_date, sys_created_by, sys_created_date, bu
          FROM z_pcdw_odm_mo_reservation;

      COMMIT;

    --- add match_code BU logic for Wistron and Quanta by zhaibo 20180115
    ---bu for WISTRON COMP, eg lenovo item is ***AA, or 0000+ odm item
    UPDATE z_pcdw_odm_mo_reservation_his
       SET BU = 'ThinkNB'
     WHERE versiondate = v_currentweek
       and ODM = 'WISTRON'
       and PKG_BASE_FUNCS.get_matnr_input(ODM_ITEM) <> PKG_BASE_FUNCS.get_matnr_input(LENOVO_ITEM)
       and LENOVO_ITEM IS NOT NULL;

    COMMIT;

    --- bu for WISTRON COMP
    UPDATE z_pcdw_odm_mo_reservation_his a
       set BU = 'IdeaNB'
     where ODM = 'WISTRON'
       AND versiondate = v_currentweek
       AND PKG_BASE_FUNCS.get_matnr_input(ODM_ITEM) = PKG_BASE_FUNCS.get_matnr_input(LENOVO_ITEM)
       AND exists ( select 1 from z_dim_component b where a.lenovo_item = b.component); --add filter to filter out odm sfg part number 20180131

    COMMIT;

    ---bu for Wistron SFG
    UPDATE z_pcdw_odm_mo_reservation_his A
       SET BU =
           (SELECT BU
              FROM z_mid_mat_util_comp_bu_family B
             WHERE A.ODM_ITEM = B.COMP
               AND A.ODM = B.WERKS
               and rownum = 1)
     WHERE EXISTS (SELECT 1
              FROM z_mid_mat_util_comp_bu_family C
             WHERE A.ODM_ITEM = C.COMP
               AND A.ODM = C.WERKS)
       AND ODM = 'WISTRON'
       AND versiondate = v_currentweek
       --AND LENOVO_ITEM IS NULL  ---lenovo item will be same with odm item for sfg product, comments on 20180131
       AND BU IS NULL;

    commit;

    --- update bu for Quanta, item souce to one BU; item mapping to multiple BU will source to think
    MERGE INTO z_pcdw_odm_mo_reservation_his t1
    USING (SELECT item, siteid, COUNT(1) cnt, MAX(bu) bu, decode(COUNT(1), 1, MAX(bu), 'IdeaNB') AS bu_f
             FROM (SELECT DISTINCT comp item, werks siteid, bu
                      FROM z_mid_mat_util_comp_bu_family
                     WHERE werks = 'QUANTA')
            GROUP BY item, siteid) t2
    ON (t1.lenovo_item = t2.item AND t1.ODM = t2.siteid)
    WHEN MATCHED THEN
      UPDATE SET t1.bu = t2.bu_f WHERE t1. versiondate = v_currentweek;

    COMMIT;
    --end change by zhaibo

    END IF;

    logger.info(l_proc_name || ' Completed');

    exitcode := 0;
  EXCEPTION
    WHEN OTHERS THEN
      exitcode := SQLCODE;
      logger.error;
      RAISE;
  END;

  PROCEDURE  prc_mrp_sccbom_init(wfl_id VARCHAR2, node_id VARCHAR2,iv_id VARCHAR2, exitcode OUT NUMBER) AS
    l_proc_name VARCHAR2(80) := g_logic_name || '.PRC_MRP_ODMBOM_INIT';
  BEGIN
    exitcode         := -20099;
    logger.wfl_id    := wfl_id;
    logger.node_id   := node_id;
    logger.log_level := 'ALL';

    logger.info(l_proc_name || ' Start');


    EXECUTE IMMEDIATE 'TRUNCATE TABLE MRP_ODM_BOM_INIT_01';-- origenal table
    EXECUTE IMMEDIATE 'TRUNCATE TABLE MRP_ODM_BOM_INIT_SPLIT';-- split child and father split result
     EXECUTE IMMEDIATE 'TRUNCATE TABLE MRP_ODM_BOM_INIT';-- final table

    logger.info(l_proc_name || ' Step1 Initial BOM');
  -- extract data from I_SCC_INTEGRATION.ODM_BOM of Bitland

    INSERT INTO MRP_ODM_BOM_INIT_01/*PRDPCGSP.ODM_BOM_INIT*/
      (ALTERNATIVE_CODE, CHILD_ITEM, EFFECTIVE_END_DATE, EFFECTIVE_START_DATE, FATHER_ITEM, ODM, ODM_CHILD_ITEM, ODM_FATHER_ITEM, PRIORITY, QUANTITY_PER, SPLIT, VERSION, APP_NAME, BU, REMARK, COMMENT1, COMMENT2, ID, CREATE_TIME, UPDATE_TIME, APP_UID, BATCH_ID, CREATE_DATE, CREATED_BY, MODIFIED_BY)
        SELECT/*+ parallel(16) */ case when odm <> 'FOXCONN' then DECODE(ALTERNATIVE_CODE,'NA',NULL,'N/A',NULL,ALTERNATIVE_CODE)
                when nvl(ALTERNATIVE_CODE,'NA') in ('NA','N/A') then DECODE(ALTERNATIVE_CODE,'NA',NULL,'N/A',NULL,ALTERNATIVE_CODE)
                when lengthb(alternative_code) < 10 then alternative_code
                else odm_father_item||'-'||dense_rank()over(partition by bu, odm order by odm_father_item , alternative_code)
            end alternative_code ,
           DECODE(REGEXP_REPLACE(CHILD_ITEM,' |_|-'),'NA',NULL,'N/A',NULL,'NA,NA',NULL,REGEXP_REPLACE(CHILD_ITEM,' |_|-')),
           EFFECTIVE_END_DATE,
           EFFECTIVE_START_DATE,
           DECODE(FATHER_ITEM,'NA',NULL,'N/A',NULL,'NA,NA',NULL,FATHER_ITEM),
           ODM,
           ODM_CHILD_ITEM,
           ODM_FATHER_ITEM,
           PRIORITY,
           QUANTITY_PER,
           SPLIT,
           VERSION,
           APP_NAME,
           BU,
           REMARK,
           COMMENT1,
           COMMENT2,
           ID,
           CREATE_TIME,
           UPDATE_TIME,
           APP_UID,
           BATCH_ID,
           CREATE_DATE,
           CREATED_BY,
           MODIFIED_BY
      FROM (select * from I_SCC_INTEGRATION.ODM_BOM@PRDCSEN  where BU<>'ISG' and bu not like'Mobile%'
      and upper(ODM) = 'BITLAND')  a
      WHERE  NOT exists (select 1 from (select * from I_SCC_INTEGRATION.ODM_BOM@PRDCSEN  where bu = 'ThinkOption'  and odm in
       ( 'USI_HUIZHOU','USI_TAIWAN','USI_VN','WISTRON','WISTRON_VN')) b where a.bu = b.bu and a.odm = b.odm );
      commit;-- confirmed with jinqq3 on not in logic-- added by yangnan13 20240716


   /* logger.info(l_proc_name || ' Step2 Split BU');*/

   /* --SPLIT BU
    --1)    WHERE BU LIKE '%/%'
    INSERT INTO   MRP_ODM_BOM_INIT \*PRDPCGSP.ODM_BOM_INIT*\
      (ALTERNATIVE_CODE, CHILD_ITEM, EFFECTIVE_END_DATE, EFFECTIVE_START_DATE, FATHER_ITEM, ODM, ODM_CHILD_ITEM, ODM_FATHER_ITEM, PRIORITY, QUANTITY_PER, SPLIT, VERSION, APP_NAME, BU, REMARK, COMMENT1, COMMENT2, ID, CREATE_TIME, UPDATE_TIME, APP_UID, BATCH_ID, CREATE_DATE, CREATED_BY, MODIFIED_BY, FLAG)
    SELECT ALTERNATIVE_CODE, CHILD_ITEM, EFFECTIVE_END_DATE, EFFECTIVE_START_DATE, FATHER_ITEM, ODM, ODM_CHILD_ITEM,
           ODM_FATHER_ITEM, PRIORITY, QUANTITY_PER, SPLIT, VERSION, APP_NAME, REGEXP_REPLACE(REGEXP_SUBSTR(BU,'[^/]+',1,LEVEL),' ') AS BU,
           REMARK, COMMENT1, COMMENT2, ID, CREATE_TIME, UPDATE_TIME, APP_UID, BATCH_ID, CREATE_DATE, CREATED_BY, MODIFIED_BY, FLAG
      FROM (SELECT ROWID RID, T.* FROM MRP_ODM_BOM_INIT_01 T
             WHERE BU LIKE '%/%') T
     CONNECT BY
      LEVEL <= REGEXP_COUNT(BU, '[^/]+')
        AND PRIOR DBMS_RANDOM.VALUE > 0
        AND PRIOR RID = RID;

    DELETE FROM MRP_ODM_BOM_INIT WHERE BU LIKE '%/%';
/*    COMMIT;*\
    logger.info(l_proc_name || ' Step3 Process WISTRON BU');
    -- Wistron logic duplicate with MRP BOM logic , so annotate this logic*/

    logger.info(l_proc_name || ' Step2  SPLIT FATHER ITEM');

    --SPLIT FATHER ITEM
     --1)   where  FATHER_ITEM LIKE '%&%'
    INSERT INTO  MRP_ODM_BOM_INIT_SPLIT
      (ALTERNATIVE_CODE, CHILD_ITEM, EFFECTIVE_END_DATE, EFFECTIVE_START_DATE, FATHER_ITEM, ODM, ODM_CHILD_ITEM, ODM_FATHER_ITEM, PRIORITY, QUANTITY_PER, SPLIT, VERSION, APP_NAME, BU, REMARK, COMMENT1, COMMENT2, ID, CREATE_TIME, UPDATE_TIME, APP_UID, BATCH_ID, CREATE_DATE, CREATED_BY, MODIFIED_BY, FLAG, RATIO)
    SELECT ALTERNATIVE_CODE, CHILD_ITEM, EFFECTIVE_END_DATE, EFFECTIVE_START_DATE, REGEXP_REPLACE(REGEXP_SUBSTR(FATHER_ITEM,'[^&]+',1,LEVEL),' ') AS FATHER_ITEM, ODM, ODM_CHILD_ITEM, ODM_FATHER_ITEM, PRIORITY, QUANTITY_PER, SPLIT, VERSION, APP_NAME, BU, REMARK, COMMENT1, COMMENT2, ID, CREATE_TIME,
           UPDATE_TIME, APP_UID, BATCH_ID, CREATE_DATE, CREATED_BY, MODIFIED_BY, FLAG, SPLIT_RATIO
      FROM (SELECT ROWID RID, T.*, 1 / (REGEXP_COUNT(FATHER_ITEM,'&') + 1) AS SPLIT_RATIO
              FROM  MRP_ODM_BOM_INIT_01 T
             WHERE FATHER_ITEM LIKE '%&%') T
     CONNECT BY
      LEVEL <= REGEXP_COUNT(FATHER_ITEM, '[^&]+')
        AND PRIOR DBMS_RANDOM.VALUE > 0
        AND PRIOR BU = BU
        AND PRIOR ODM = ODM
        AND PRIOR RID = RID;
    --2)   where  FATHER_ITEM LIKE '%,%'
    INSERT INTO  MRP_ODM_BOM_INIT_SPLIT
      (ALTERNATIVE_CODE, CHILD_ITEM, EFFECTIVE_END_DATE, EFFECTIVE_START_DATE, FATHER_ITEM, ODM, ODM_CHILD_ITEM, ODM_FATHER_ITEM, PRIORITY, QUANTITY_PER, SPLIT, VERSION, APP_NAME, BU, REMARK, COMMENT1, COMMENT2, ID, CREATE_TIME, UPDATE_TIME, APP_UID, BATCH_ID, CREATE_DATE, CREATED_BY, MODIFIED_BY, FLAG, RATIO)
    SELECT ALTERNATIVE_CODE, CHILD_ITEM, EFFECTIVE_END_DATE, EFFECTIVE_START_DATE, REGEXP_REPLACE(REGEXP_SUBSTR(FATHER_ITEM,'[^,]+',1,LEVEL),' ') AS FATHER_ITEM, ODM, ODM_CHILD_ITEM, ODM_FATHER_ITEM, PRIORITY, QUANTITY_PER, SPLIT, VERSION, APP_NAME, BU, REMARK, COMMENT1, COMMENT2,
           ID, CREATE_TIME, UPDATE_TIME, APP_UID, BATCH_ID, CREATE_DATE, CREATED_BY, MODIFIED_BY, FLAG, SPLIT_RATIO
      FROM (SELECT ROWID RID, T.*, 1 / (REGEXP_COUNT(FATHER_ITEM,',') + 1) AS SPLIT_RATIO
              FROM  MRP_ODM_BOM_INIT_01 T
             WHERE FATHER_ITEM LIKE '%,%') T
     CONNECT BY
      LEVEL <= REGEXP_COUNT(FATHER_ITEM, '[^,]+')
        AND PRIOR DBMS_RANDOM.VALUE > 0
        AND PRIOR BU = BU
        AND PRIOR ODM = ODM
        AND PRIOR RID = RID;
     --3)   where  FATHER_ITEM LIKE '%/%'
    INSERT INTO  MRP_ODM_BOM_INIT_SPLIT
      (ALTERNATIVE_CODE, CHILD_ITEM, EFFECTIVE_END_DATE, EFFECTIVE_START_DATE, FATHER_ITEM, ODM, ODM_CHILD_ITEM, ODM_FATHER_ITEM, PRIORITY, QUANTITY_PER, SPLIT, VERSION, APP_NAME, BU, REMARK, COMMENT1, COMMENT2, ID, CREATE_TIME, UPDATE_TIME, APP_UID, BATCH_ID, CREATE_DATE, CREATED_BY, MODIFIED_BY, FLAG, RATIO)
    SELECT ALTERNATIVE_CODE, CHILD_ITEM, EFFECTIVE_END_DATE, EFFECTIVE_START_DATE, REGEXP_REPLACE(REGEXP_SUBSTR(FATHER_ITEM,'[^/]+',1,LEVEL),' ') AS FATHER_ITEM, ODM, ODM_CHILD_ITEM, ODM_FATHER_ITEM, PRIORITY, QUANTITY_PER, SPLIT, VERSION, APP_NAME, BU, REMARK, COMMENT1, COMMENT2,
           ID, CREATE_TIME, UPDATE_TIME, APP_UID, BATCH_ID, CREATE_DATE, CREATED_BY, MODIFIED_BY, FLAG, SPLIT_RATIO
      FROM (SELECT ROWID RID, T.*, 1 / (REGEXP_COUNT(FATHER_ITEM,'/') + 1) AS SPLIT_RATIO
              FROM  MRP_ODM_BOM_INIT_01 T
             WHERE FATHER_ITEM LIKE '%/%') T
     CONNECT BY
      LEVEL <= REGEXP_COUNT(FATHER_ITEM, '[^/]+')
        AND PRIOR DBMS_RANDOM.VALUE > 0
        AND PRIOR BU = BU
        AND PRIOR ODM = ODM
        AND PRIOR RID = RID;
        COMMIT;

     logger.info(l_proc_name || ' Step3  SPLIT FATHER ITEM DONE');
    DELETE FROM  MRP_ODM_BOM_INIT_SPLIT WHERE REGEXP_LIKE(FATHER_ITEM,'/|,|&');
     COMMIT;

     logger.info(l_proc_name || ' Step5  DEAL WITH CHILD ITEM START');
    --SPLIT CHILD_ITEM
    -- 1)WHERE CHILD_ITEM LIKE '%,%'
      INSERT INTO  MRP_ODM_BOM_INIT_SPLIT
      (ALTERNATIVE_CODE, CHILD_ITEM, EFFECTIVE_END_DATE, EFFECTIVE_START_DATE, FATHER_ITEM, ODM, ODM_CHILD_ITEM, ODM_FATHER_ITEM,
      PRIORITY, QUANTITY_PER, SPLIT, VERSION, APP_NAME, BU, REMARK, COMMENT1, COMMENT2, ID, CREATE_TIME, UPDATE_TIME,
      APP_UID, BATCH_ID, CREATE_DATE, CREATED_BY, MODIFIED_BY, FLAG, RATIO)
    SELECT ALTERNATIVE_CODE, REGEXP_REPLACE(REGEXP_SUBSTR(CHILD_ITEM,'[^,]+',1,LEVEL),' ') AS child_item,
    EFFECTIVE_END_DATE, EFFECTIVE_START_DATE, FATHER_ITEM, ODM, ODM_CHILD_ITEM,
    ODM_FATHER_ITEM, PRIORITY, QUANTITY_PER, SPLIT, VERSION, APP_NAME, BU, REMARK, COMMENT1, COMMENT2,
           ID, CREATE_TIME, UPDATE_TIME, APP_UID, BATCH_ID, CREATE_DATE, CREATED_BY, MODIFIED_BY, FLAG, SPLIT_RATIO
      FROM (SELECT ROWID RID, T.*, 1 / (REGEXP_COUNT(child_item,',') + 1) AS SPLIT_RATIO
              FROM MRP_ODM_BOM_INIT_01  T
             WHERE CHILD_ITEM LIKE '%,%' ) T
     CONNECT BY
      LEVEL <= REGEXP_COUNT(CHILD_ITEM, '[^,]+')
        AND PRIOR DBMS_RANDOM.VALUE > 0
        AND PRIOR BU = BU
        AND PRIOR ODM = ODM
        AND PRIOR RID = RID;
        COMMIT;
   -- 2)WHERE CHILD_ITEM LIKE '% &%'
          INSERT INTO  MRP_ODM_BOM_INIT_SPLIT
      (ALTERNATIVE_CODE, CHILD_ITEM, EFFECTIVE_END_DATE, EFFECTIVE_START_DATE, FATHER_ITEM, ODM, ODM_CHILD_ITEM, ODM_FATHER_ITEM,
      PRIORITY, QUANTITY_PER, SPLIT, VERSION, APP_NAME, BU, REMARK, COMMENT1, COMMENT2, ID, CREATE_TIME, UPDATE_TIME,
      APP_UID, BATCH_ID, CREATE_DATE, CREATED_BY, MODIFIED_BY, FLAG, RATIO)
    SELECT ALTERNATIVE_CODE, REGEXP_REPLACE(REGEXP_SUBSTR(CHILD_ITEM,'[^&]+',1,LEVEL),' ') AS child_item,
    EFFECTIVE_END_DATE, EFFECTIVE_START_DATE, FATHER_ITEM, ODM, ODM_CHILD_ITEM,
    ODM_FATHER_ITEM, PRIORITY, QUANTITY_PER, SPLIT, VERSION, APP_NAME, BU, REMARK, COMMENT1, COMMENT2,
           ID, CREATE_TIME, UPDATE_TIME, APP_UID, BATCH_ID, CREATE_DATE, CREATED_BY, MODIFIED_BY, FLAG, SPLIT_RATIO
      FROM (SELECT ROWID RID, T.*, 1 / (REGEXP_COUNT(child_item,',') + 1) AS SPLIT_RATIO
              FROM MRP_ODM_BOM_INIT_01  T
             WHERE CHILD_ITEM LIKE '%&%' ) T
     CONNECT BY
      LEVEL <= REGEXP_COUNT(CHILD_ITEM, '[^&]+')
        AND PRIOR DBMS_RANDOM.VALUE > 0
        AND PRIOR BU = BU
        AND PRIOR ODM = ODM
        AND PRIOR RID = RID;
        COMMIT;


      logger.info(l_proc_name || ' Step5  INSERT INTO CHILD ITEM DONE');
     DELETE FROM  MRP_ODM_BOM_INIT_SPLIT WHERE REGEXP_LIKE(CHILD_ITEM,',|&|');
      COMMIT;
      logger.info(l_proc_name || ' Step5  DELETE CHILD ITEM DONE');

--summarize all data -- added by yangnan13 ---20240726
--1) insert all data
       INSERT INTO MRP_ODM_BOM_INIT
       (ALTERNATIVE_CODE, CHILD_ITEM, EFFECTIVE_END_DATE, EFFECTIVE_START_DATE, FATHER_ITEM, ODM, ODM_CHILD_ITEM, ODM_FATHER_ITEM, PRIORITY, QUANTITY_PER, SPLIT, VERSION, APP_NAME, BU, REMARK, COMMENT1, COMMENT2, ID, CREATE_TIME, UPDATE_TIME, APP_UID, BATCH_ID,
       CREATE_DATE, CREATED_BY, MODIFIED_BY, FLAG, PCT, RATIO, IS_SPLIT)
       select ALTERNATIVE_CODE, CHILD_ITEM, EFFECTIVE_END_DATE, EFFECTIVE_START_DATE, FATHER_ITEM, ODM, ODM_CHILD_ITEM,
       ODM_FATHER_ITEM, PRIORITY, QUANTITY_PER, SPLIT, VERSION, APP_NAME, BU, REMARK, COMMENT1, COMMENT2, ID, CREATE_TIME, UPDATE_TIME, APP_UID, BATCH_ID,
       CREATE_DATE, CREATED_BY, MODIFIED_BY, FLAG, PCT, RATIO, null as IS_SPLIT from  MRP_ODM_BOM_INIT_01 a;
       commit;

--2) insert split data
         INSERT INTO MRP_ODM_BOM_INIT
       (ALTERNATIVE_CODE, CHILD_ITEM, EFFECTIVE_END_DATE, EFFECTIVE_START_DATE, FATHER_ITEM, ODM, ODM_CHILD_ITEM, ODM_FATHER_ITEM, PRIORITY, QUANTITY_PER, SPLIT, VERSION, APP_NAME, BU, REMARK, COMMENT1, COMMENT2, ID, CREATE_TIME, UPDATE_TIME, APP_UID, BATCH_ID,
       CREATE_DATE, CREATED_BY, MODIFIED_BY, FLAG, PCT, RATIO, IS_SPLIT)
       select ALTERNATIVE_CODE, CHILD_ITEM, EFFECTIVE_END_DATE, EFFECTIVE_START_DATE, FATHER_ITEM, ODM, ODM_CHILD_ITEM,
       ODM_FATHER_ITEM, PRIORITY, QUANTITY_PER, SPLIT, VERSION, APP_NAME, BU, REMARK, COMMENT1, COMMENT2, ID, CREATE_TIME, UPDATE_TIME, APP_UID, BATCH_ID,
       CREATE_DATE, CREATED_BY, MODIFIED_BY, FLAG, PCT, RATIO, null as IS_SPLIT from  MRP_ODM_BOM_INIT_SPLIT a;
       commit;

--3) delete connect data
        delete from  MRP_ODM_BOM_INIT WHERE REGEXP_LIKE(CHILD_ITEM,',');
        commit;

       delete from  MRP_ODM_BOM_INIT WHERE REGEXP_LIKE(CHILD_ITEM,'&');
        commit;

        DELETE FROM  MRP_ODM_BOM_INIT  WHERE REGEXP_LIKE(FATHER_ITEM,'/|,|&');
        COMMIT;



     logger.info(l_proc_name || ' Step5.5  ALL BOM DATA INSERT DONE');

    logger.info(l_proc_name || ' Step6 Clear Data_1');
    --Clear Data logic
    --1) delete father item = child item and father item is not null
    --ITEM1 -> ITEM1 and EXISTS ITEM1->OTHER ITEM
    DELETE FROM MRP_ODM_BOM_INIT T1
     WHERE T1.FATHER_ITEM = T1.CHILD_ITEM
       AND T1.FATHER_ITEM IS NOT NULL
       AND EXISTS (SELECT 1
                     FROM MRP_ODM_BOM_INIT T2
                    WHERE T1.BU = T2.BU
                      AND T1.ODM = T2.ODM
                      AND T1.FATHER_ITEM = T2.FATHER_ITEM
                      AND T1.CHILD_ITEM <> T2.CHILD_ITEM);
      COMMIT;

     logger.info(l_proc_name || ' Step6 Clear Data_2');
    --ITEM1 -> ITEM1 and NOT EXISTS ITEM1->OTHER ITEM
    --2) delete father father_item = child _item
    UPDATE MRP_ODM_BOM_INIT T1
       SET CHILD_ITEM = NULL
     WHERE FATHER_ITEM = CHILD_ITEM
       AND FATHER_ITEM IS NOT NULL
       AND NOT EXISTS (SELECT 1
                         FROM MRP_ODM_BOM_INIT T2
                        WHERE T1.BU = T2.BU
                          AND T1.ODM = T2.ODM
                          AND T1.FATHER_ITEM = T2.FATHER_ITEM
                          AND T1.CHILD_ITEM <> T2.CHILD_ITEM);
         commit;

 logger.info(l_proc_name || ' Step6 Clear Data_3');

    MERGE INTO MRP_ODM_BOM_INIT A
    USING (SELECT T1.ROWID AS RID,
                  DECODE(REGEXP_INSTR(FATHER_ITEM, '^[[:digit:]]+$'), 1, LPAD(FATHER_ITEM, 18, '0'),FATHER_ITEM) FATHER_ITEM,
                  DECODE(REGEXP_INSTR(CHILD_ITEM, '^[[:digit:]]+$'), 1, LPAD(CHILD_ITEM, 18, '0'),CHILD_ITEM) CHILD_ITEM
             FROM MRP_ODM_BOM_INIT T1) B
       ON (A.ROWID = B.RID)
     WHEN MATCHED THEN
       UPDATE SET A.FATHER_ITEM = B.FATHER_ITEM,
                  A.CHILD_ITEM = B.CHILD_ITEM;
         commit;
 logger.info(l_proc_name || ' Step6 Clear Data_4');
    MERGE INTO MRP_ODM_BOM_INIT A
    USING (SELECT T1.ROWID AS RID,
                  DECODE(LENGTH(FATHER_ITEM),7,LPAD(FATHER_ITEM, 12, '0'),FATHER_ITEM) FATHER_ITEM,
                  DECODE(LENGTH(CHILD_ITEM),7,LPAD(CHILD_ITEM, 12, '0'),CHILD_ITEM) CHILD_ITEM
             FROM MRP_ODM_BOM_INIT T1) B
       ON (A.ROWID = B.RID)
     WHEN MATCHED THEN
       UPDATE SET A.FATHER_ITEM = B.FATHER_ITEM,
                  A.CHILD_ITEM = B.CHILD_ITEM;
     commit;
 logger.info(l_proc_name || ' Step6 Clear Data_4');
    --ALL charactor is 0
    UPDATE MRP_ODM_BOM_INIT
       SET FATHER_ITEM = NULL
     WHERE REGEXP_INSTR(FATHER_ITEM,'[^0]+') = 0;
     commit;

 logger.info(l_proc_name || ' Step6 Clear Data_5');
    UPDATE MRP_ODM_BOM_INIT
       SET CHILD_ITEM = NULL
     WHERE REGEXP_INSTR(CHILD_ITEM,'[^0]+') = 0;
    COMMIT;


    DBMS_STATS.GATHER_TABLE_STATS
                                 (OWNNAME   => 'PRDSPOEMTHK',
                                  TABNAME      => 'MRP_ODM_BOM_INIT',
                                  ESTIMATE_PERCENT => '100',
                                  METHOD_OPT       => 'for all columns size auto',
                                  NO_INVALIDATE    => FALSE,
                                  DEGREE           => 4,
                                  CASCADE          => TRUE);


    --DELETE CYCLE BOM
     logger.info(l_proc_name || ' Step6 Clear Data_6');
     EXECUTE IMMEDIATE 'TRUNCATE TABLE  mrp_odm_cyclebom';-- added by yangnan13 for code optimization 20240801
      EXECUTE IMMEDIATE 'TRUNCATE TABLE  z_sccbom_sbb';-- added by yangnan13 for code optimization 20240801
      insert into z_sccbom_sbb
      SELECT SBB,sysdate FROM PRDPCDW.Z_DSP_DIM_SBB@PRDMDMN_19C_NEW where SBB like 'SBB%';
      commit;
     insert into mrp_odm_cyclebom
     (LV, ISLEAF, ISCYCLE,ALTERNATIVE_CODE, CHILD_ITEM, EFFECTIVE_END_DATE, EFFECTIVE_START_DATE,
     FATHER_ITEM, ODM, ODM_CHILD_ITEM, ODM_FATHER_ITEM, PRIORITY, QUANTITY_PER, SPLIT, VERSION, APP_NAME, BU,
     REMARK, COMMENT1, COMMENT2, ID, CREATE_TIME, UPDATE_TIME, APP_UID, BATCH_ID,
     CREATE_DATE, CREATED_BY, MODIFIED_BY, FLAG, PCT, RATIO, IS_SPLIT)
     select * from (
     SELECT LEVEL, CONNECT_BY_ISLEAF AS ISLEAF, CONNECT_BY_ISCYCLE AS ISCYCLE, T.*
                            FROM MRP_ODM_BOM_INIT T
                           START WITH FATHER_ITEM IN (SELECT  SBB FROM z_sccbom_sbb)
                           CONNECT BY NOCYCLE PRIOR ODM_CHILD_ITEM = ODM_FATHER_ITEM AND PRIOR ODM = ODM AND PRIOR BU = BU)
        WHERE ISCYCLE = 1;
        COMMIT;

    DELETE FROM MRP_ODM_BOM_INIT T1
     WHERE EXISTS(SELECT 1
                    FROM mrp_odm_cyclebom T2
                   WHERE T1.BU = T2.BU
                     AND T1.ODM = T2.ODM
                     AND T1.FATHER_ITEM = T2.FATHER_ITEM
                     AND T1.CHILD_ITEM = T2.CHILD_ITEM
                     AND T1.ODM_FATHER_ITEM = T2.ODM_FATHER_ITEM
                     AND T1.ODM_CHILD_ITEM = T2.ODM_CHILD_ITEM);
                     commit;
   logger.info(l_proc_name || ' Step6 Clear Data_7');
     DELETE FROM MRP_ODM_BOM_INIT A
     WHERE LENGTH(CHILD_ITEM) > 10
       AND ODM IN ('WISTRON','WISTRON ZS')
       AND NOT EXISTS (SELECT 1 FROM MRP_ODM_BOM_INIT B WHERE B.FATHER_ITEM = A.CHILD_ITEM AND A.ODM = B.ODM)
       AND EXISTS (SELECT 1 FROM PRDPCDW.Z_DSP_DIM_COMPONENT@PRDMDMN_19C_NEW WHERE COMPONENT = SUBSTR(A.CHILD_ITEM,1,10))
       AND EXISTS (SELECT 1 FROM MRP_ODM_BOM_INIT B WHERE B.FATHER_ITEM = SUBSTR(A.CHILD_ITEM,1,10) AND A.ODM = B.ODM)
       AND NOT EXISTS (SELECT 1 FROM MRP_ODM_BOM_INIT B
                        WHERE B.FATHER_ITEM = SUBSTR(A.CHILD_ITEM,1,10)
                          AND A.ODM = B.ODM
                          AND SUBSTR(B.FATHER_ITEM,1,10) = SUBSTR(B.CHILD_ITEM,1,10));
     commit;
   logger.info(l_proc_name || ' Step6 Clear Data_8');
    DELETE FROM MRP_ODM_BOM_INIT A
     WHERE CHILD_ITEM LIKE 'SBB%'
       AND NOT EXISTS (SELECT 1 FROM PRDPCDW.Z_DSP_DIM_PRODUCT@PRDMDMN_19C_NEW
                        WHERE PRODUCT = NVL(A.FATHER_ITEM,A.ODM_FATHER_ITEM))
       AND NOT EXISTS (SELECT 1 FROM PRDPCDW.Z_DSP_DIM_COMPONENT@PRDMDMN_19C_NEW
                        WHERE COMPONENT = NVL(A.FATHER_ITEM,A.ODM_FATHER_ITEM)
                          AND HIER1_CODE = 'ZERS');
    commit;

   logger.info(l_proc_name || ' Step6 Clear Data_9');
    --DELETE DOULBE FOR WISTRON
    DELETE FROM MRP_ODM_BOM_INIT T1
     WHERE T1.ROWID IN (SELECT RID
                          FROM (SELECT T1.ROWID AS RID,
                                       ROW_NUMBER() OVER(PARTITION BY FATHER_ITEM, CHILD_ITEM, ODM_FATHER_ITEM, ODM_CHILD_ITEM, ALTERNATIVE_CODE, PRIORITY, SPLIT ORDER BY T1.ROWID) RN
                                  FROM MRP_ODM_BOM_INIT T1
                                 WHERE ODM = 'WISTRON'
                                   AND BU IS NULL)
                         WHERE RN > 1);
    COMMIT;

   logger.info(l_proc_name || ' Step6 Clear Data_10');
    MERGE INTO MRP_ODM_BOM_INIT A
    USING (  WITH ODM_BOM AS (
               SELECT DISTINCT BU, ODM, ODM_FATHER_ITEM, ODM_CHILD_ITEM, ALTERNATIVE_CODE
                 FROM MRP_ODM_BOM_INIT
                WHERE ALTERNATIVE_CODE IS NOT NULL),
                  ISSUE_DATA AS (
               SELECT BU, ODM, ODM_FATHER_ITEM, ALTERNATIVE_CODE
                 FROM MRP_ODM_BOM_INIT T1-- changed by yangnan13 20240801
                GROUP BY BU, ODM, ODM_FATHER_ITEM, ALTERNATIVE_CODE
               HAVING COUNT(*) = 1)
           SELECT T1.ROWID AS RID
             FROM MRP_ODM_BOM_INIT T1
            INNER JOIN ISSUE_DATA T2
               ON T1.BU = T2.BU AND T1.ODM = T2.ODM AND T1.ODM_FATHER_ITEM = T2.ODM_FATHER_ITEM AND T1.ALTERNATIVE_CODE = T2.ALTERNATIVE_CODE) B
       ON (A.ROWID = B.RID)
     WHEN MATCHED THEN
       UPDATE SET A.ALTERNATIVE_CODE = NULL;
       commit;


    logger.info(l_proc_name || ' Completed');
    exitcode := 0;

   EXCEPTION
    WHEN OTHERS THEN
      exitcode := SQLCODE;
      logger.error;
      RAISE;

  END;

  PROCEDURE prc_odm_bom(wfl_id VARCHAR2, node_id VARCHAR2, iv_id VARCHAR2, exitcode OUT NUMBER) AS
    l_proc_name   VARCHAR2(40) := g_logic_name || '.PRC_ODM_BOM';
    l_crr_version DATE := get_curr_version('BSMRP');
  BEGIN
    exitcode         := -20099;
    logger.wfl_id    := wfl_id;
    logger.node_id   := node_id;
    logger.log_level := 'ALL';

    pkg_base_funcs.init_log_run(g_logic_name || ':' || iv_id);

     ------add by yangnan13 20240819 begin for Bitland SCC BOM prepare ---------------------
    prc_mrp_sccbom_init(wfl_id, node_id,iv_id, exitcode);
    ------add by yangnan13 20240819 end for Bitland SCC BOM prepare-----------------------

    logger.info(l_proc_name || ' Start');


    EXECUTE IMMEDIATE 'truncate table  z_mid_source_odmbom';--add by yangnan13 20240819 for Bitland
    EXECUTE IMMEDIATE 'truncate table z_mid_imp_odm_bom';
    EXECUTE IMMEDIATE 'truncate table z_pcdw_odm_bom';

     /* *************************************************************************************
    Backgroud : SA&V project partitial done , Bitland should use sav data --- added by yangnan13 20240724
    ****************************************************************************************/
    --step 1 insert into SCC Bitland bom source added by yangnan13 20240819
     insert into z_mid_source_odmbom
    (ODM, FATHER_LENOVOPN, FATHER_ODMPN, SON_LENOVOPN, SON_ODMPN, QTYPER, ALPGR, ALPRF,
    EFFSTARTDATE, EFFENDDATE, ODM_CREATED_DATE, SYS_CREATED_BY,SYS_CREATED_DATE)
    select ODM, FATHER_ITEM,ODM_FATHER_ITEM,CHILD_ITEM,ODM_CHILD_ITEM,QUANTITY_PER,ALTERNATIVE_CODE as ALPGR,
    PRIORITY as ALPRF,EFFECTIVE_START_DATE,EFFECTIVE_END_DATE,CREATE_TIME,'SCC' as  SYS_CREATED_BY,sysdate as SYS_CREATED_DATE
    from MRP_ODM_BOM_INIT a WHERE ODM_FATHER_ITEM IS NOT NULL
    and exists ( select 1 from (SELECT DISTINCT PVALUE,BOM_SOURCE
                                    FROM z_ui_conf_parameter
                                   WHERE pdomain = 'ODM_CODE') b
                                   where  b.BOM_SOURCE = 'SCC'
                                   and upper(a.odm) = upper(b. PVALUE));
    COMMIT;

    DELETE FROM imp_odm_bom WHERE son_lenovopn = '41U4034';

    --add by xuml7 for wangqun10 NEC bom issue 20240507
    update prdpcdw.imp_odm_bom
    set FATHER_LENOVOPN = ''
    where FATHER_ODMPN = '000000087084100002';

    --add by xuml7 for yuzf3, for which one lenovopn have more than 1 odmpn, need mapping to unique ODMPN
    update imp_odm_bom t
       set SON_ODMPN = SON_LENOVOPN
     where SON_LENOVOPN in ('SB20N60893',
                            'SB20N60890',
                            'SB20N60928',
                            'SB20N60965',
                            'SB20N60961',
                            'SB20N60962')
       and FATHER_LENOVOPN like 'SBB%';

    delete from imp_odm_bom a
     where exists
     (select 1
              from (select T.ROWID AS RID,
                           row_number() over(partition by ODM, FATHER_LENOVOPN order by FATHER_ODMPN) as rn
                      from imp_odm_bom t
                     where FATHER_LENOVOPN in ('SB20N60893',
                                               'SB20N60890',
                                               'SB20N60928',
                                               'SB20N60965',
                                               'SB20N60961',
                                               'SB20N60962')) b
             where a.rowid = b.rid
               and b.rn > 1);
    commit;

    --add by xuml7 for wangcheng15,when the same odm father & odm just have one alt part, then update the alpgr to null 20230829
    update PRDPCDW.IMP_ODM_BOM a
       set alpgr = ''
     where exists (select 1
              from (SELECT count(SON_ODMPN) over(partition by odm, father_odmpn, alpgr) cnt,
                           t.rowid as rid
                      FROM PRDPCDW.IMP_ODM_BOM T
                     WHERE alpgr is not null) b
             where a.rowid = b.rid
               and b.cnt = 1);
    commit;

    update prdpcdw.imp_odm_bom
    set father_lenovopn =replace(father_lenovopn,'/','')
    where odm='AVC';
    COMMIT;
    UPDATE PRDPCDW.IMP_ODM_BOM
    SET SON_LENOVOPN=REPLACE(SON_LENOVOPN,'/','')
    WHERE ODM='AVC';
    COMMIT;

    delete from imp_odm_bom
    where father_lenovopn  in ('SBB0Q20867','SBB0Q17719')
    and son_odmpn in ('2A585K700-G98-G')
    and alpgr not in(1,7)
    and odm='FOXCONN'  ;

    COMMIT;

    UPDATE imp_odm_bom
    SET SON_LENOVOPN=''
    WHERE NVL(SON_LENOVOPN,' ') IN('SM30U64434','SM30U64433')
    AND ODM='LCFC';

    UPDATE imp_odm_bom
    SET FATHER_LENOVOPN=''
    WHERE NVL(FATHER_LENOVOPN,' ') IN('SM30U64434','SM30U64433')
    AND ODM='LCFC';
    COMMIT;
    -- added by yangnan13 for Bitalnad bom 20240819
     INSERT INTO z_pcdw_odm_bom
      (odm, father_lenovopn, father_odmpn, son_lenovopn, son_odmpn, qtyper, alpgr, alprf, effstartdate,
       effenddate, odm_created_date, sys_created_by, sys_created_date)
      SELECT /*+ parallel(a 4)*/
      DISTINCT DECODE(upper(odm),'FOXCONNL6','FOXCONN',odm), TRIM(father_lenovopn), TRIM(father_odmpn), TRIM(son_lenovopn), TRIM(son_odmpn),
               qtyper, alpgr, substr(alprf,1,4),
               case when trunc(sys_created_date) = trunc(effstartdate) then trunc(effstartdate,'IW')
                      else effstartdate
               end as effstartdate, --add by zy3
               effenddate, odm_created_date, g_logic_name, SYSDATE
        FROM z_mid_source_odmbom a
        where upper(a.ODM) = 'BITLAND'
       and instr(son_lenovopn, '_') = 0
          OR son_lenovopn IS NULL;
    COMMIT;


    INSERT INTO z_pcdw_odm_bom
      (odm, father_lenovopn, father_odmpn, son_lenovopn, son_odmpn, qtyper, alpgr, alprf, effstartdate,
       effenddate, odm_created_date, sys_created_by, sys_created_date)
      SELECT /*+ parallel(a 4)*/
      DISTINCT DECODE(upper(odm),'FOXCONNL6','FOXCONN',odm), TRIM(father_lenovopn), TRIM(father_odmpn), TRIM(son_lenovopn), TRIM(son_odmpn),
               qtyper, alpgr, substr(alprf,1,4),
               case when trunc(sys_created_date) = trunc(effstartdate) then trunc(effstartdate,'IW')
                      else effstartdate
               end as effstartdate, --add by zy3
               effenddate, odm_created_date, g_logic_name, SYSDATE
        FROM (select * from prdpcdw.imp_odm_bom where ODM <>'BITLAND') a -- ADDED BY YANGNAN13 FOR BITLAND 20240902
       WHERE instr(son_lenovopn, '_') = 0
          OR son_lenovopn IS NULL;--  added by yangnan13 for VN 20241230;
    COMMIT;


INSERT INTO Z_PCDW_ODM_BOM
  (ODM,
   FATHER_LENOVOPN,
   FATHER_ODMPN,
   SON_LENOVOPN,
   SON_ODMPN,
   QTYPER,
   ALPGR,
   ALPRF,
   EFFSTARTDATE,
   EFFENDDATE)
  SELECT ODM,
         SUBSTR(FATHER_LENOVOPN,INSTR(FATHER_LENOVOPN,'_')+1,10),
         SUBSTR(FATHER_LENOVOPN,INSTR(FATHER_LENOVOPN,'_')+1,10),
         SUBSTR(FATHER_LENOVOPN,1,instr(father_lenovopn,'_')-1),
         SUBSTR(FATHER_LENOVOPN,1,instr(father_lenovopn,'_')-1),
         1,
         ALPGR,
         ALPRF,
         min(EFFSTARTDATE),
         max(EFFENDDATE)
    FROM IMP_ODM_BOM
   WHERE instr(father_lenovopn, '_') > 0
     and instr(father_lenovopn, 'SB20') > 0
     AND instr(father_lenovopn, 'SBB') > 0
     group by ODM,
         SUBSTR(FATHER_LENOVOPN,INSTR(FATHER_LENOVOPN,'_')+1,10),
         SUBSTR(FATHER_LENOVOPN,INSTR(FATHER_LENOVOPN,'_')+1,10),
         SUBSTR(FATHER_LENOVOPN,1,instr(father_lenovopn,'_')-1),
         SUBSTR(FATHER_LENOVOPN,1,instr(father_lenovopn,'_')-1),
         ALPGR,
         ALPRF
UNION ALL
      SELECT ODM,
         SUBSTR(FATHER_LENOVOPN,1,instr(father_lenovopn,'_')-1),
         FATHER_ODMPN,
         SON_LENOVOPN,
         SON_ODMPN,
         QTYPER,
         ALPGR,
         ALPRF,
         min(EFFSTARTDATE),
         max(EFFENDDATE)
    FROM IMP_ODM_BOM
   WHERE instr(father_lenovopn, '_') > 0
     and instr(father_lenovopn, 'SB20') > 0
     AND instr(father_lenovopn, 'SBB') > 0
     group by ODM,
         SUBSTR(FATHER_LENOVOPN,1,instr(father_lenovopn,'_')-1),     --modify by wangke15 at 20200427
         FATHER_ODMPN,
         SON_LENOVOPN,
         SON_ODMPN,
         QTYPER,
         ALPGR,
         ALPRF;
     COMMIT;

    --Add by qinying4 for LCFC error BOM start
    UPDATE Z_PCDW_ODM_BOM
    SET SON_LENOVOPN=''
    WHERE ODM='LCFC'
    AND SON_LENOVOPN='SP10K38391'
    AND FATHER_LENOVOPN IN('SSA0S71773','SSA0S71774','SSA0T37581','SW10A11646','SW10K97446','5D10K81086');
    COMMIT;
    --Add by qinying4 for LCFC error BOM end

    --need split son_lenovopn
    INSERT INTO z_mid_imp_odm_bom
      (odm, father_lenovopn, father_odmpn, son_lenovopn, son_odmpn, qtyper, alpgr, alprf, effstartdate,
       effenddate, odm_created_date, sys_created_by, sys_created_date)
      SELECT /*+ parallel(a 4)*/
       DECODE(upper(odm),'FOXCONNL6','FOXCONN',odm), TRIM(father_lenovopn), TRIM(father_odmpn), TRIM(son_lenovopn), TRIM(son_odmpn), qtyper,
       alpgr, alprf, effstartdate, effenddate, odm_created_date, sys_created_by, sys_created_date
        FROM prdpcdw.imp_odm_bom a
       WHERE instr(son_lenovopn, '_') > 0;
    COMMIT;
/*    delete from z_mid_imp_odm_bom
     where son_lenovopn like '%SW10T06429%'
       and odm = 'HUAQIN';
    commit;*/--add by xuml7 20210823
    --split son_lenovopn
    INSERT INTO z_pcdw_odm_bom
      (odm, father_lenovopn, father_odmpn, son_lenovopn, son_odmpn, qtyper, alpgr, alprf, effstartdate,
       effenddate, odm_created_date, sys_created_by, sys_created_date)
      SELECT DISTINCT odm, father_lenovopn, father_odmpn,
                      regexp_substr(son_lenovopn, '[^_]+', 1, rn) son_lenovopn, son_odmpn, qtyper, alpgr,
                      substr(alprf,1,4), effstartdate, effenddate, odm_created_date, g_logic_name, SYSDATE
        FROM z_mid_imp_odm_bom a, (SELECT LEVEL rn FROM dual CONNECT BY LEVEL <= 40) b --add by qinying for LCFC multi-son_lenovopn
       WHERE rn <= regexp_count(nvl(son_lenovopn, 'X'), '[^_]+');

    COMMIT;

    --add by xuml7 20240515 for father lenovopn case like: father_lenovopn = 'SBG0W62216_SBG0W62223'
    INSERT INTO z_pcdw_odm_bom
      (odm, father_lenovopn, father_odmpn, son_lenovopn, son_odmpn, qtyper, alpgr, alprf, effstartdate,
       effenddate, odm_created_date, sys_created_by, sys_created_date)
     SELECT DISTINCT odm, regexp_substr(father_lenovopn, '[^_]+', 1, rn) father_lenovopn, father_odmpn,
                          son_lenovopn, son_odmpn, qtyper, alpgr,
                          substr(alprf,1,4), effstartdate, effenddate, odm_created_date, g_logic_name, SYSDATE
      FROM z_pcdw_odm_bom a,
           (SELECT LEVEL rn FROM dual CONNECT BY LEVEL <= 40) b --add by qinying for LCFC multi-son_lenovopn
     WHERE rn <= regexp_count(nvl(father_lenovopn, 'X'), '[^_]+')
       and a.father_lenovopn in (select son_lenovopn from z_mid_imp_odm_bom)
       and (a.FATHER_LENOVOPN like 'SBG%' OR a.FATHER_LENOVOPN LIKE 'SB2%')
       and FATHER_LENOVOPN NOT LIKE '%SBB%';
    COMMIT;

    --delete lcfc mtm -sbb bom

    DELETE FROM z_pcdw_odm_bom a
     WHERE EXISTS (SELECT 1
              FROM z_dim_sbb_bs c
             WHERE a.son_lenovopn = c.sbb
               AND a.odm = 'LCFC' -- add by zy3, LITEON MTM BOM need ADD SBB son part
            ) ---add by xujc1 exclude LCFC MTM-SBB BOM
          /*--Add by qinying for MTY shell
           and not exists(
                  select 1 from z_dim_component_bs b
                  where a.father_lenovopn=b.component
                  and b.hier5_code in('SHELL','BS_GBM')) */
    ;

  --delete lcfc ISSUE BOM 20181101
  delete from z_pcdw_odm_bom WHERE FATHER_LENOVOPN IN ('5SB0N24830') and odm ='LCFC';--,'5SB0N24828');

    COMMIT;

    --add by zy3 2018-4-4, solve wistron idea/think bom double count issue
    delete from z_pcdw_odm_bom a
    where length(son_lenovopn ) >10 and odm in ('WISTRON','WISTRON ZS','WISTRON_VN_NB','WISTRON_VN')-- added by yangnan13 for VN 20250102
    and not exists (select 1 from z_pcdw_odm_bom b where b.father_lenovopn = a.son_lenovopn and a.odm = b.odm)
    and exists (select 1 from z_dim_component_bs where component = substr(a.son_lenovopn,1,10))
    and exists (select 1 from z_pcdw_odm_bom b where b.father_lenovopn = substr(a.son_lenovopn,1,10) and a.odm = b.odm)
    and not exists (select 1 from z_pcdw_odm_bom b where b.father_lenovopn = substr(a.son_lenovopn,1,10)
                    and a.odm = b.odm and substr(b.father_lenovopn,1,10) = substr(b.son_lenovopn,1,10));

    commit;
    --end

    FOR i IN 7 .. 12 LOOP
      UPDATE z_pcdw_odm_bom a
         SET son_lenovopn = substr(a.son_lenovopn, 0, i), flag = 'Y'
       WHERE upper(a.odm) IN ('WISTRON', 'WISTRON ZS','WISTRON_VN_NB','WISTRON_VN')-- added by yangnan13 for VN 20250102
         AND EXISTS (SELECT 1 FROM z_dim_component_bs b WHERE substr(a.son_lenovopn, 0, i) = b.component)
         AND NOT EXISTS (SELECT 1 FROM z_dim_component_bs c WHERE a.son_lenovopn = c.component)
         AND flag IS NULL;

    END LOOP;

    commit;

    --solve the huaqin son-lenovopn is NA,by zy3 2018-7-5
    update z_pcdw_odm_bom a
    set son_lenovopn = null
    where odm in('HUAQIN','COMPAL','COMPAL_VN') and nvl(son_lenovopn,' ') in('NA','N/A');

    update z_pcdw_odm_bom a
    set father_lenovopn = null
    where odm in('HUAQIN','COMPAL','COMPAL_VN') and nvl(father_lenovopn,' ') in('NA','N/A');

    commit;
    --end by zy3 2018-7-5

    --add by zy3 for odm bom issue,2018-2-8
    --when father lenovopn = son lenovopn, update the son lenovopn = null
    /*update z_pcdw_odm_bom
    set son_lenovopn = null
    where father_lenovopn = son_lenovopn
    and son_odmpn is not null;
    */
     --2 cases
     --1. delete the father = son and exists others father bom
     --example: -- son_lenovopn = '5D10K85755'
    /* update z_pcdw_odm_bom a  ---TESTING FOR VN added by yangnan13 for VN
    set father_lenovopn = FATHER_ODMPN,
        SON_LENOVOPN = SON_ODMPN
    where odm in('COMPAL_VN') and father_lenovopn = 'NA';
    commit;*/
     
     
     
    delete z_pcdw_odm_bom a
    where father_lenovopn = son_lenovopn
    and son_odmpn is not null
    and  exists (select 1 from z_pcdw_odm_bom b
                    where a.son_lenovopn = b.son_lenovopn
                    and a.odm = b.odm
                    and a.father_lenovopn <> b.father_lenovopn);
    commit;
    --2. update the son lenovo pn is null
    update z_pcdw_odm_bom a
    set son_lenovopn = null
    where father_lenovopn = son_lenovopn
    and son_odmpn is not null
    and not exists (select 1 from z_pcdw_odm_bom b
                    where a.son_lenovopn = b.son_lenovopn
                    and a.odm = b.odm
                    and a.father_lenovopn <> b.father_lenovopn);

    commit;
    --end

/*    --solve the huaqin son-lenovopn is NA,by zy3 2018-7-5
    update z_pcdw_odm_bom a
    set son_lenovopn = null
    where odm in('HUAQIN','COMPAL') and nvl(son_lenovopn,' ') in('NA','N/A');

    update z_pcdw_odm_bom a
    set father_lenovopn = null
    where odm in('HUAQIN','COMPAL') and nvl(father_lenovopn,' ') in('NA','N/A');

    commit;
    --end by zy3 2018-7-5*/

    --solve SB50->SBB,by XIECHAO 2018-10-11
    UPDATE z_pcdw_odm_bom t1
       SET son_lenovopn = NULL
     WHERE odm IN ('QUANTA'/*, 'COMPAL'*/)
       AND EXISTS (SELECT 1 FROM z_dim_sbb t2 WHERE t1.son_lenovopn = t2.sbb)
       AND EXISTS (SELECT 1
              FROM z_dim_component_bs t3
             WHERE t3.component = nvl(t1.father_lenovopn,t1. father_odmpn)
               AND t3.hier5_code = 'BAREBONE');
    COMMIT;
    --Add by qinying4 for compal special case
    delete from Z_CONF_COMPAL_BOM;

    INSERT INTO z_conf_compal_bom
      SELECT family, item AS sbb, SYSDATE
        FROM z_mid_fg_sbb_fcst
       WHERE itemtype = 'CTO_SBB'
         AND bu = 'IdeaNB'
      UNION
      SELECT family, b.sbb, SYSDATE
        FROM z_mid_fg_sbb_fcst a
        JOIN z_v_dim_bom_mtm b
          ON a.planning_item_id = b.product
       WHERE itemtype = 'MTM'
         AND bu = 'IdeaNB';
    COMMIT;

    --solve the SBB->SBB bom  structure
    delete from z_pcdw_odm_bom a
    where  son_lenovopn like 'SBB%'
    and not exists (select 1 from z_dim_product_bs where product = nvl(father_lenovopn,father_odmpn))
    and not exists (select 1 from z_dim_component_bs where component = nvl(father_lenovopn,father_odmpn) and hier1_code = 'ZERS')
    --and nvl(sys_created_by,'PKG_PCDW_TO_MRP') = 'PKG_PCDW_TO_MRP'
    and not exists (select 1 from z_conf_compal_bom b where nvl(a.father_lenovopn,' ')=b.sbb)
    and a.odm<>'COMPAL'
    ;

    commit;

    -- solve the quanta 1 sbb mapping multiple odmpn case
    --ADD BY XUML7 to deal with SB20 mapping to different odmpn
    EXECUTE IMMEDIATE 'truncate table z_mid_odm_bom_multi_map_bak';

    INSERT INTO z_mid_odm_bom_multi_map_bak
      (odm, lenovopn, odmpn)
      SELECT DISTINCT odm, father_lenovopn, father_odmpn
        FROM (SELECT odm, father_lenovopn, father_odmpn,
                      COUNT(*) over(PARTITION BY odm, father_lenovopn) AS row_cnt,
                      COUNT(*) over(PARTITION BY odm, father_lenovopn, father_odmpn) AS odmpn_cnt
                 FROM z_pcdw_odm_bom a
                WHERE sys_created_by = g_logic_name
                  AND effenddate >= l_crr_version --added by zy3
               )
       WHERE row_cnt / odmpn_cnt > 1
         AND father_odmpn IS NOT NULL
         AND father_lenovopn IS NOT NULL;
    COMMIT;

    update z_pcdw_odm_bom
set FATHER_LENOVOPN = ''
where (FATHER_LENOVOPN, FATHER_ODMPN) in
(select lenovopn, odmpn
  from z_mid_odm_bom_multi_map_bak A
 where lenovopn like 'SB2%' and odmpn not like '451%'
   AND ODM LIKE 'LCFC'
   AND EXISTS (SELECT 1
          FROM z_mid_odm_bom_multi_map_bak B
         where a.odm = b.odm
           and a.lenovopn = b.lenovopn
           and b.odmpn like '451%')
   AND EXISTS(select 1 from z_pcdw_odm_inventory_his c
              where  versionyear = gv_versionyear
                  and versionweek = gv_versionweek
                  and a.lenovopn = c.lenovopn and a.odm = c.odm)
                  ) and odm = 'LCFC';
update z_pcdw_odm_bom
set FATHER_LENOVOPN = ''
where (SON_LENOVOPN, SON_ODMPN) in
(select lenovopn, odmpn
  from z_mid_odm_bom_multi_map_bak A
 where lenovopn like 'SB2%' and odmpn not like '451%'
   AND ODM LIKE 'LCFC'
   AND EXISTS (SELECT 1
          FROM z_mid_odm_bom_multi_map_bak B
         where a.odm = b.odm
           and a.lenovopn = b.lenovopn
           and b.odmpn like '451%')
   AND EXISTS(select 1 from z_pcdw_odm_inventory_his c
              where  versionyear = gv_versionyear
                  and versionweek = gv_versionweek
                  and a.lenovopn = c.lenovopn and a.odm = c.odm)) and odm = 'LCFC';
COMMIT;

    --end add

    EXECUTE IMMEDIATE 'truncate table z_mid_odm_bom_multi_map';

    INSERT INTO z_mid_odm_bom_multi_map
      (odm, lenovopn, odmpn)
      SELECT DISTINCT odm, father_lenovopn, father_odmpn
        FROM (SELECT odm, father_lenovopn, father_odmpn,
                      COUNT(*) over(PARTITION BY odm, father_lenovopn) AS row_cnt,
                      COUNT(*) over(PARTITION BY odm, father_lenovopn, father_odmpn) AS odmpn_cnt
                 FROM z_pcdw_odm_bom a
                WHERE sys_created_by = g_logic_name
                  AND effenddate >= l_crr_version --added by zy3
               )
       WHERE row_cnt / odmpn_cnt > 1
         AND father_odmpn IS NOT NULL
         AND father_lenovopn IS NOT NULL;

    COMMIT;

    INSERT INTO z_pcdw_odm_bom
      (odm, father_lenovopn, father_odmpn, son_lenovopn, son_odmpn, qtyper, alpgr, alprf, effstartdate,
       effenddate, odm_created_date, sys_created_by, sys_created_date)
      SELECT DISTINCT odm, father_lenovopn, NULL AS father_odmpn, father_odmpn AS son_lenovopn,
                      father_odmpn AS son_odmpn, 1 AS qtyper, '1' AS alpgr, '99' AS alprf,
                      to_date('1971-1-1', 'yyyy-mm-dd') AS effstartdate,
                      to_date('9999-12-31', 'yyyy-mm-dd') AS effenddate, SYSDATE AS odm_created_date,
                      'BOM_ADJUST_INSERT' AS sys_created_by, SYSDATE
        FROM (SELECT odm, father_lenovopn, father_odmpn,
                      COUNT(*) over(PARTITION BY odm, father_lenovopn) AS row_cnt,
                      COUNT(*) over(PARTITION BY odm, father_lenovopn, father_odmpn) AS odmpn_cnt
                 FROM z_pcdw_odm_bom a
                WHERE sys_created_by = g_logic_name
                  AND effenddate >= l_crr_version --added by zy3
               )
       WHERE row_cnt / odmpn_cnt > 1
         AND father_odmpn IS NOT NULL
         AND father_lenovopn IS NOT NULL;

    UPDATE z_pcdw_odm_bom a
       SET father_lenovopn = father_odmpn, sys_created_by = 'BOM_ADJUST_UPDATE'
     WHERE sys_created_by = g_logic_name
       AND (odm, father_lenovopn, father_odmpn) IN
           (SELECT odm, father_lenovopn, father_odmpn
              FROM (SELECT odm, father_lenovopn, father_odmpn,
                            COUNT(*) over(PARTITION BY odm, father_lenovopn) AS row_cnt,
                            COUNT(*) over(PARTITION BY odm, father_lenovopn, father_odmpn) AS odmpn_cnt
                       FROM z_pcdw_odm_bom a
                      WHERE sys_created_by = g_logic_name
                        AND effenddate >= l_crr_version --added by zy3
                     )
             WHERE row_cnt / odmpn_cnt > 1
               AND father_odmpn IS NOT NULL
               AND father_lenovopn IS NOT NULL);

    COMMIT;

    --add by zy3 for fix the father -child - father bom
    --and odmpn has child part
    UPDATE z_pcdw_odm_bom a
       SET son_lenovopn = NULL, sys_created_by = sys_created_by || '_FIX'
     WHERE son_lenovopn IS NOT NULL
       AND son_odmpn IS NOT NULL
       AND son_lenovopn <> son_odmpn
       AND EXISTS (SELECT 1
              FROM z_mid_odm_bom_multi_map b
             WHERE a.son_lenovopn = b.lenovopn
               AND a.son_odmpn = b.odmpn
               AND a.odm = b.odm)
       AND EXISTS (SELECT 1
              FROM z_pcdw_odm_bom b
             WHERE a.odm = b.odm
               AND a.son_odmpn = b.father_odmpn);
    COMMIT;

    --delete inactive bom--added by zy3
    DELETE FROM z_pcdw_odm_bom a WHERE effenddate < l_crr_version;

    DELETE FROM z_pcdw_odm_bom
    where  odm='COMPAL'
    AND FATHER_LENOVOPN IN('5SB0R47677','5SB0R47678');
    COMMIT;
    --Add by qinying for nested loop BOM 20190405
    DELETE FROM Z_PCDW_ODM_BOM
    WHERE FATHER_LENOVOPN in ('SA10R16873','SB10K97655','SB10K97656','SA10R16867','SA10R16871')
    and ODM='LCFC';

    COMMIT;

    INSERT INTO z_pcdw_odm_bom
      (odm, father_lenovopn, father_odmpn, son_lenovopn, son_odmpn, qtyper, alpgr, alprf, effstartdate,
       effenddate, odm_created_date, sys_created_by, sys_created_date)
   select odm, father_lenovopn, father_odmpn, son_lenovopn, son_odmpn, qtyper, alpgr, substr(alprf,1,4), effstartdate,
       effenddate, odm_created_date, sys_created_by, sys_created_date
   FROM prdpcdw.imp_odm_bom a
   where exists(select 1 from z_mid_salesorder b
                     where a.father_lenovopn = b.item
                     and brand='X420')
      and EXISTS (SELECT 1
              FROM z_dim_sbb_bs c
             WHERE a.son_lenovopn = c.sbb
               )
        AND a.odm = 'LCFC' ;
    commit;
    ---20191226 guoqiang
    INSERT INTO z_pcdw_odm_bom
      (odm, father_lenovopn, father_odmpn, son_lenovopn, son_odmpn, qtyper, alpgr, alprf, effstartdate,
       effenddate, odm_created_date, sys_created_by, sys_created_date)
  select /*+parallel(8)*/
    a.odm, a.father_lenovopn, a.father_odmpn, b.son_lenovopn, b.son_odmpn, b.qtyper, b.alpgr, substr(b.alprf,1,4), b.effstartdate,
       b.effenddate, b.odm_created_date, b.sys_created_by, b.sys_created_date
    from  prdpcdw.imp_odm_bom a  ,
    prdpcdw.imp_odm_bom b
   where  a.odm = 'LCFC'
      AND a.SON_LENOVOPN LIKE 'SBB%'
      AND a.FATHER_LENOVOPN not like 'SBB%'
      and a.odm=b.odm
      and a.son_odmpn=b.father_odmpn
      and not exists(select 1 from z_pcdw_odm_bom c where a.odm=c.odm
       and nvl(a.father_lenovopn,'null')=nvl(c.father_lenovopn,'null')
       and nvl(a.father_odmpn,'null')=nvl(c.father_odmpn,'null')
       and nvl( b.son_lenovopn,'null')=nvl(c.son_lenovopn,'null')
       and nvl(b.son_odmpn,'null')=nvl(c.son_odmpn,'null')
        );
        commit;

        update z_pcdw_odm_bom
        set son_lenovopn=REPLACE(son_lenovopn, 'ForLCFC', '')
        where odm='BITLAND' and instr(son_lenovopn,'ForLCFC')>0;
        commit;--add by xurt1 for error PN in BITLAND

    delete from z_pcdw_odm_bom
     where odm = 'LCFC'
       and father_lenovopn in (select component from z_dim_component_bs)
       and son_lenovopn in (select sbb from z_dim_sbb_bs)
       and father_lenovopn not in
           (select shellpn
              from shellpn_list
             where siteid = 'LCFC'
               and siteid = 'LCFC');
    commit;

    delete from z_pcdw_odm_bom
     where odm = 'LCFC'
       and father_lenovopn in (select shellpn
                                 from shellpn_list
                                where siteid = 'LCFC'
                                  and siteid = 'LCFC')
       and nvl(son_lenovopn, son_odmpn) not in
           (select sbb from z_dim_sbb_bs);
    commit;
    --add by xurt1 for item->sbb->x89
    execute immediate 'truncate table z_pcdw_odm_bom_bak_bak';
    insert into z_pcdw_odm_bom_bak_bak
    select * from z_pcdw_odm_bom where odm='COMPAL';
    COMMIT;

    delete from z_pcdw_odm_bom a where odm = 'LCFC'
    and a.FATHER_LENOVOPN in (select distinct item from z_ui_bs_share where display_type not in ('DS','DB','FLEX') and commodity = 'Touchpanel');
    commit;


    -- sepecila logic for VN project which should add 36% level  to bom to solve out sourcing issue-- added by yangnan13 20250116
    delete from Z_PCDW_ODM_BOM_CVN;
    commit;

    insert into Z_PCDW_ODM_BOM_CVN
      (ODM,
       FATHER_LENOVOPN,
       FATHER_ODMPN,
       SON_LENOVOPN,
       SON_ODMPN,
       QTYPER,
       ALPGR,
       ALPRF,
       EFFSTARTDATE,
       EFFENDDATE,
       ODM_CREATED_DATE,
       SYS_CREATED_BY,
       SYS_CREATED_DATE,
       FLAG)
      select FROM_ODM,
             FATHER_LENOVOPN,
             FATHER_ODMPN,
             SON_LENOVOPN,
             SON_ODMPN,
             QTYPER,
             ALPGR,
             ALPRF,
             EFFSTARTDATE,
             EFFENDDATE,
             ODM_CREATED_DATE,
             SYS_CREATED_BY,
             SYS_CREATED_DATE,
             FLAG
        from (select FROM_ODM,
                     LENOVO_PN AS FATHER_LENOVOPN,
                     LENOVO_PN AS FATHER_ODMPN,
                     ODM_PN SON_LENOVOPN,
                     ODM_PN SON_ODMPN,
                     1 AS QTYPER,
                     NULL AS ALPGR,
                     1 AS ALPRF,
                     ODM_CREATED_DATE AS EFFSTARTDATE,
                     DATE '9999-12-31' AS EFFENDDATE,
                     CREATE_TIME as ODM_CREATED_DATE,
                     'odm_imp_source@PRDCSEN' as SYS_CREATED_BY,
                     CREATE_TIME AS SYS_CREATED_DATE,
                     'VN' as flag,
                     row_number() over(partition by FROM_ODM, LENOVO_PN, ODM_PN, ODM_CREATED_DATE order by FROM_ODM, LENOVO_PN, ODM_PN, ODM_CREATED_DATE, VERSION desc) rn
                FROM I_SCC_INTEGRATION.odm_imp_source@PRDCSEN
               WHERE from_odm = 'COMPAL_VN'
                 and LENOVO_PN like '36%'
                 --and ODM_PN like '45%'
                 and substr(version, 1, 8) =
                             (select max(substr(version, 1, 8))
                                from I_SCC_INTEGRATION.odm_imp_source@PRDCSEN)
                 )
       where rn = 1;


    commit;
    
    --LCFC_bom dummy level
    insert into Z_PCDW_ODM_BOM_CVN
      (ODM,
       FATHER_LENOVOPN,
       FATHER_ODMPN,
       SON_LENOVOPN,
       SON_ODMPN,
       QTYPER,
       ALPGR,
       ALPRF,
       EFFSTARTDATE,
       EFFENDDATE,
       ODM_CREATED_DATE,
       SYS_CREATED_BY,
       SYS_CREATED_DATE,
       FLAG)
      select FROM_ODM,
             FATHER_LENOVOPN,
             FATHER_ODMPN,
             SON_LENOVOPN,
             SON_ODMPN,
             QTYPER,
             ALPGR,
             ALPRF,
             EFFSTARTDATE,
             EFFENDDATE,
             ODM_CREATED_DATE,
             SYS_CREATED_BY,
             SYS_CREATED_DATE,
             FLAG
        from (select FROM_ODM,
                     LENOVO_PN AS FATHER_LENOVOPN,
                     LENOVO_PN AS FATHER_ODMPN,
                     ODM_PN SON_LENOVOPN,
                     ODM_PN SON_ODMPN,
                     1 AS QTYPER,
                     NULL AS ALPGR,
                     1 AS ALPRF,
                     ODM_CREATED_DATE AS EFFSTARTDATE,
                     DATE '9999-12-31' AS EFFENDDATE,
                     CREATE_TIME as ODM_CREATED_DATE,
                     'odm_imp_source@PRDCSEN' as SYS_CREATED_BY,
                     CREATE_TIME AS SYS_CREATED_DATE,
                     'VN' as flag,
                     row_number() over(partition by FROM_ODM, LENOVO_PN, ODM_PN, ODM_CREATED_DATE order by FROM_ODM, LENOVO_PN, ODM_PN, ODM_CREATED_DATE, VERSION desc) rn
                FROM I_SCC_INTEGRATION.odm_imp_source@PRDCSEN
               WHERE from_odm = 'LCFC_VN'
                 and LENOVO_PN like 'PV%'
                 and substr(version, 1, 8) =
                             (select max(substr(version, 1, 8))
                                from I_SCC_INTEGRATION.odm_imp_source@PRDCSEN)
                 )
       where rn = 1;


    commit;

    delete from Z_PCDW_ODM_BOM where flag = 'VN';
    commit;

    insert into  Z_PCDW_ODM_BOM
    (ODM, FATHER_LENOVOPN, FATHER_ODMPN, SON_LENOVOPN, SON_ODMPN, QTYPER, ALPGR, ALPRF, EFFSTARTDATE,
    EFFENDDATE, ODM_CREATED_DATE, SYS_CREATED_BY, SYS_CREATED_DATE, FLAG)
    select ODM, FATHER_LENOVOPN, FATHER_ODMPN, SON_LENOVOPN, SON_ODMPN, QTYPER, ALPGR, ALPRF, EFFSTARTDATE,
    EFFENDDATE, ODM_CREATED_DATE, SYS_CREATED_BY, SYS_CREATED_DATE, FLAG from Z_PCDW_ODM_BOM_CVN;
    commit;


    ------add by lp 20180813 begin---------------------
    prc_odm_bom_bak(wfl_id, node_id, iv_id, exitcode);
    ------add by lp 20180813 end-----------------------

    ------add by zhongyao 2022-03-02 begin---------------------
    prc_odm_bom_bak4_cpsp(wfl_id, node_id, iv_id, exitcode);
    ------add by zhongyao 2022-03-02 end-----------------------

--add wanggang38 2025-1-13 for bom huaqin TO huaqin_vn  for VN
insert into z_pcdw_odm_bom(ODM, FATHER_LENOVOPN, FATHER_ODMPN, SON_LENOVOPN, SON_ODMPN, QTYPER, ALPGR, ALPRF, EFFSTARTDATE, EFFENDDATE, ODM_CREATED_DATE, SYS_CREATED_BY, SYS_CREATED_DATE, FLAG)
select 'HUAQIN_VN' ODM, FATHER_LENOVOPN, FATHER_ODMPN, SON_LENOVOPN, SON_ODMPN, QTYPER, ALPGR, ALPRF, EFFSTARTDATE, EFFENDDATE, ODM_CREATED_DATE, SYS_CREATED_BY, SYS_CREATED_DATE, FLAG
 from z_pcdw_odm_bom a where a.odm='HUAQIN';
COMMIT;

    logger.info(l_proc_name || ' Completed');

    exitcode := 0;
  EXCEPTION
    WHEN OTHERS THEN
      exitcode := SQLCODE;
      logger.error;
      RAISE;
  END;

  PROCEDURE prc_odm_fru(wfl_id VARCHAR2, node_id VARCHAR2, iv_id VARCHAR2, exitcode OUT NUMBER) AS
    l_proc_name VARCHAR2(40) := g_logic_name || '.PRC_ODM_FRU';
  BEGIN
    exitcode         := -20099;
    logger.wfl_id    := wfl_id;
    logger.node_id   := node_id;
    logger.log_level := 'ALL';

    pkg_base_funcs.init_log_run(g_logic_name || ':' || iv_id);

    logger.info(l_proc_name || ' Start');

    EXECUTE IMMEDIATE 'truncate table z_pcdw_odm_fru';

    EXECUTE IMMEDIATE 'truncate table z_pcdw_odm_fru_fcst';

    INSERT INTO z_pcdw_odm_fru
      (odm, odmpn, lenovopn, basicname, description, description_chs, material_type, effstartdate,
       sys_created_by, sys_created_date)
      SELECT upper(odm), odmpn, lenovopn, basicname, description, description_chs, material_type, effstartdate,
             g_logic_name, SYSDATE
        FROM prdpcdw.imp_odm_fru;

    COMMIT;

    INSERT INTO z_pcdw_odm_fru_fcst
      (fru_qty, lenovo_item, odm, odm_item, req_date, sys_created_by, sys_created_date)
      SELECT fru_qty, lenovo_item, upper(odm), odm_item, req_date, sys_created_by, SYSDATE
        FROM prdpcdw.imp_odm_fru_fcst
        where trunc(sysdate,'iw') = trunc(sys_created_date,'iw') --add by zy3,for odm miss txt file
        ;

    COMMIT;

    --WISTRON COMP, eg lenovo item is ***AA by zhaibo 20180209
    UPDATE z_pcdw_odm_fru_fcst a
       SET lenovo_item = substr(a.odm_item, 0, 7)
     WHERE a.odm IN ('WISTRON', 'WISTRON ZS')
       AND EXISTS (SELECT 1 FROM z_dim_component_bs b WHERE substr(a.odm_item, 0, 7) = b.component)
       AND NOT EXISTS (SELECT 1 FROM z_dim_component_bs c WHERE a.lenovo_item = c.component);

    COMMIT;

    UPDATE z_pcdw_odm_fru_fcst a
       SET lenovo_item = substr(a.odm_item, 0, 10)
     WHERE a.odm IN ('WISTRON', 'WISTRON ZS')
       AND EXISTS (SELECT 1 FROM z_dim_component_bs b WHERE substr(a.odm_item, 0, 10) = b.component)
       AND NOT EXISTS (SELECT 1 FROM z_dim_component_bs c WHERE a.lenovo_item = c.component);

    COMMIT;

    --- add match_code BU logic for Wistron by zhaibo 20180209
    ---bu for WISTRON COMP, eg lenovo item is ***AA, or 0000+ odm item
    UPDATE z_pcdw_odm_fru_fcst
       SET BU = 'ThinkNB'
     WHERE ODM = 'WISTRON'
       and PKG_BASE_FUNCS.get_matnr_input(ODM_ITEM) <> PKG_BASE_FUNCS.get_matnr_input(LENOVO_ITEM)
       and LENOVO_ITEM IS NOT NULL;

    COMMIT;

    --- bu for WISTRON COMP
    UPDATE z_pcdw_odm_fru_fcst a
       set BU = 'IdeaNB'
     where ODM = 'WISTRON'
       AND PKG_BASE_FUNCS.get_matnr_input(ODM_ITEM) = PKG_BASE_FUNCS.get_matnr_input(LENOVO_ITEM)
       AND exists ( select 1 from z_dim_component b where a.lenovo_item = b.component); --add filter to filter out odm sfg part number 20180131

    COMMIT;

    ---bu for Wistron SFG
    UPDATE z_pcdw_odm_fru_fcst A
       SET BU =
           (SELECT BU
              FROM z_mid_mat_util_comp_bu_family B
             WHERE A.ODM_ITEM = B.COMP
               AND A.ODM = B.WERKS
               and rownum = 1)
     WHERE EXISTS (SELECT 1
              FROM z_mid_mat_util_comp_bu_family C
             WHERE A.ODM_ITEM = C.COMP
               AND A.ODM = C.WERKS)
       AND ODM = 'WISTRON'
       --AND LENOVO_ITEM IS NULL  ---lenovo item will be same with odm item for sfg product, comments on 20180131
       AND BU IS NULL;

    commit;
    ---END CHANGE

    IF to_number(to_char(SYSDATE, 'D', 'NLS_DATE_LANGUAGE = American')) = v_snapdate
    THEN
      EXECUTE IMMEDIATE 'ALTER TABLE z_pcdw_odm_fru_fcst_HIS TRUNCATE SUBPARTITION P_' ||
                        to_char(v_currentweek, 'YYYY') || '_W_' || to_char(v_currentweek, 'WW');

      INSERT INTO z_pcdw_odm_fru_fcst_his
        (versionyear, versionweek, versiondate, fru_qty, lenovo_item, odm, odm_item, req_date, sys_created_by,
         sys_created_date)
        SELECT trunc(v_currentweek, 'yyyy'), to_char(v_currentweek, 'ww'), v_currentweek, fru_qty, lenovo_item,
               odm, odm_item, req_date, sys_created_by, sys_created_date
          FROM z_pcdw_odm_fru_fcst;

      COMMIT;

    END IF;

    logger.info(l_proc_name || ' Completed');

    exitcode := 0;
  EXCEPTION
    WHEN OTHERS THEN
      exitcode := SQLCODE;
      logger.error;
      RAISE;
  END;

  PROCEDURE prc_odm_fg_shipment(wfl_id VARCHAR2, node_id VARCHAR2, iv_id VARCHAR2, exitcode OUT NUMBER) AS
    l_proc_name VARCHAR2(40) := g_logic_name || '.PRC_ODM_FG_SHIPMENT';
  BEGIN
    exitcode         := -20099;
    logger.wfl_id    := wfl_id;
    logger.node_id   := node_id;
    logger.log_level := 'ALL';

    pkg_base_funcs.init_log_run(g_logic_name || ':' || iv_id);

    logger.info(l_proc_name || ' Start');

    EXECUTE IMMEDIATE 'truncate table z_pcdw_odm_fg_shipment';

    INSERT INTO z_pcdw_odm_fg_shipment
      (odm, po_id, po_line_id, item, ship_date, ship_qty, odm_created_date, sys_created_by, sys_created_date)
      SELECT upper(odm), po_id, po_line_id, item, ship_date, ship_qty, odm_created_date, g_logic_name, SYSDATE
        FROM prdpcdw.imp_odm_fg_shipment;

    COMMIT;

    logger.info(l_proc_name || ' Completed');

    exitcode := 0;
  EXCEPTION
    WHEN OTHERS THEN
      exitcode := SQLCODE;
      logger.error;
      RAISE;
  END;

  PROCEDURE prc_odm_mtmpo_map(wfl_id VARCHAR2, node_id VARCHAR2, iv_id VARCHAR2, exitcode OUT NUMBER) AS
    l_proc_name VARCHAR2(40) := g_logic_name || '.TBD2';
  BEGIN
    exitcode         := -20099;
    logger.wfl_id    := wfl_id;
    logger.node_id   := node_id;
    logger.log_level := 'ALL';

    pkg_base_funcs.init_log_run(g_logic_name || ':' || iv_id);

    logger.info(l_proc_name || ' Start');

    EXECUTE IMMEDIATE 'truncate table Z_PCDW_ODM_MTMPO_MAP';

    INSERT INTO z_pcdw_odm_mtmpo_map
      (odm, mtm, po_id, po_line, odm_created_date, sys_created_by, sys_created_date)
      SELECT upper(odm), mtm, po_id, po_line, odm_created_date, g_logic_name, SYSDATE
        FROM prdpcdw.imp_odm_mtmpo_map;

    COMMIT;

    logger.info(l_proc_name || ' Completed');

    exitcode := 0;
  EXCEPTION
    WHEN OTHERS THEN
      exitcode := SQLCODE;
      logger.error;
      RAISE;
  END;

  PROCEDURE prc_odm_source(wfl_id VARCHAR2, node_id VARCHAR2, iv_id VARCHAR2, exitcode OUT NUMBER) AS
    l_proc_name VARCHAR2(40) := g_logic_name || '.PRC_ODM_SOURCE';

  BEGIN
    exitcode         := -20099;
    logger.wfl_id    := wfl_id;
    logger.node_id   := node_id;
    logger.log_level := 'ALL';

    pkg_base_funcs.init_log_run(g_logic_name || ':' || iv_id);

    logger.info(l_proc_name || ' Start');

    --      EXECUTE IMMEDIATE 'truncate table z_pcdw_odm_source';
    ----------modified by lp 20180802 begin-----------------------------------------------
   /* DELETE FROM z_pcdw_odm_source a
     WHERE EXISTS (SELECT 1
              FROM prdpcdw.imp_odm_source b
             WHERE a.odm = b.odm
               AND a.odmpn = b.odmpn
               AND a.lenovopn = b.lenovopn);
    COMMIT;

    INSERT INTO z_pcdw_odm_source
      (odm, odmpn, lenovopn, from_odm, to_odm, odm_created_date, sys_created_by, sys_created_date)
      SELECT upper(odm), odmpn, lenovopn, upper(from_odm), upper(to_odm), odm_created_date, g_logic_name,
             SYSDATE
        FROM prdpcdw.imp_odm_source;
    COMMIT;*/
    
    DELETE FROM z_pcdw_odm_source a
     WHERE EXISTS (SELECT 1
              FROM prdpcdw.imp_odm_source b
             WHERE a.odm = b.odm);
    COMMIT;

    INSERT INTO z_pcdw_odm_source
      (odm, odmpn, lenovopn, from_odm, to_odm, odm_created_date, sys_created_by, sys_created_date,SBB)
      SELECT upper(odm), odmpn, LENOVOPN, upper(from_odm), decode(upper(to_odm),'WISTRONZS','WISTRON ZS',upper(to_odm)), odm_created_date, g_logic_name,
             SYSDATE,SBB
        FROM prdpcdw.imp_odm_source s
       where exists (select 1 from mst_sitemaster m
                      where upper(s.from_odm) = upper(m.siteid));
    COMMIT;


    DELETE FROM z_pcdw_odm_source  where  upper(FROM_ODM)
    in ('WISTRON_VN_NB','LCFC_VN','COMPAL_VN','HUAQIN_VN') ;
    commit;

      INSERT INTO z_pcdw_odm_source
        (odm,
         odmpn,
         lenovopn,
         from_odm,
         to_odm,
         odm_created_date,
         sys_created_by,
         sys_created_date,
         SBB)
        select ODM,
               ODMPN,
               LENOVOPN,
               FROM_ODM,
               TO_ODM,
               ODM_CREATED_DATE,
               G_LOGIC_NAME,
               SYSDATE,
               SBB
          from (SELECT UPPER(ODM) ODM,
                       ODM_PN odmpn,
                       LENOVO_PN LENOVOPN,
                       upper(FROM_ODM) from_odm,
                       upper(TO_ODM) TO_ODM,
                       ODM_CREATED_DATE,
                       SBB,
                       row_number() over(partition by ODM, ODM_PN, LENOVO_PN, FROM_ODM, TO_ODM, ODM_CREATED_DATE, SBB order by ODM, ODM_PN, LENOVO_PN, FROM_ODM, TO_ODM, ODM_CREATED_DATE, SBB, VERSION desc) rn
                  FROM I_SCC_INTEGRATION.odm_imp_source@prdcsen s
                  where exists (select 1 from (select ODM,max(VERSION) version from  I_SCC_INTEGRATION.odm_imp_source@prdcsen group by ODM) b
                            where s.version=b.version and s.ODM=b.ODM)
                   and exists
                 (select 1
                          from mst_sitemaster m
                         where upper(s.from_odm) = upper(m.siteid))
                   AND upper(FROM_ODM) in ('WISTRON_VN_NB',
                                           'LCFC_VN',
                                           'COMPAL_VN',
                                           'HUAQIN_VN'
                                           ))
         where rn = 1;
    COMMIT;

    --add by xurt1 for customer upload the null lenovopn in odm source
    delete from z_pcdw_odm_source where lenovopn is null and odm = 'WINGTECH';

    update z_pcdw_odm_source a
    set lenovopn=(SELECT distinct b.FATHER_LENOVOPN   -- added distinct for yangnan13 for VN 20241226
                                 FROM PRDPCDW.IMP_ODM_BOM b
                                WHERE EXISTS (SELECT 1 FROM Z_DIM_SBB_BS WHERE FATHER_LENOVOPN=SBB)
                                and a.ODMPN = b.son_odmpn  -- added by yangnan13 20240914
                                START WITH SON_ODMPN IN
                                           (SELECT ODMPN
                                              FROM PRDPCDW.IMP_ODM_SOURCE
                                             WHERE LENOVOPN IS NULL)
                               CONNECT BY PRIOR FATHER_ODMPN = SON_ODMPN AND PRIOR ODM=ODM)
                               where lenovopn is null;
                               commit;

    ----------modified by lp 20180802 end-------------------------------------------------

    --add by xuml7 20240110
    update z_pcdw_odm_source
    set odmpn = 'SB21C42346'
    where odmpn = '713100840081';
    commit;
    --end



    --add the source from odm model to tmapi bom by zy3
    EXECUTE IMMEDIATE 'truncate table z_mid_outsourcing_bom';

    INSERT INTO z_mid_outsourcing_bom
      (odm, odmpn, lenovopn, from_odm, to_odm, measure, sys_created_date, SBB)
      SELECT a.odm, odmpn, lenovopn, from_odm, to_odm, b.measure, sys_created_date, SBB
        FROM z_pcdw_odm_source a, z_v_item_desc b
       WHERE a.lenovopn = b.item
        and exists (select 1 from mst_sitemaster c where a.from_odm = c.siteid);

    COMMIT;
    -- as vn senario only has odmpn ,so default set them measrue = 'BAREBONE'-- yangnan13 20241226
       INSERT INTO z_mid_outsourcing_bom
      (odm, odmpn, lenovopn, from_odm, to_odm, measure, sys_created_date, SBB)
      SELECT a.odm, odmpn, lenovopn, from_odm, to_odm, 'VN_BAREBONE' as measure, sysdate as sys_created_date, SBB
        FROM z_pcdw_odm_source a
        WHERE UPPER(FROM_ODM) IN ('LCFC_VN','COMPAL_VN','HUAQIN_VN','WISTRON_VN_NB')
        and exists (select 1 from mst_sitemaster c where upper(a.from_odm)= upper(c.siteid))
        and not exists ( select 1 from z_mid_outsourcing_bom  t where t.odm = a.odm  and t.odmpn = a.odmpn and t.from_odm= t.from_odm);-- aviod duplicate for VN yangnan13 20250104
        commit;

    logger.info(l_proc_name || ' Completed');

    exitcode := 0;
  EXCEPTION
    WHEN OTHERS THEN
      exitcode := SQLCODE;
      logger.error;
      RAISE;
  END;

  PROCEDURE prc_fru_openpo_demand(wfl_id VARCHAR2, node_id VARCHAR2, iv_id VARCHAR2, exitcode OUT NUMBER) AS
    l_proc_name VARCHAR2(40) := g_logic_name || '.PRC_FRU_OPENPO_DEMAND';
  BEGIN
    exitcode         := -20099;
    logger.wfl_id    := wfl_id;
    logger.node_id   := node_id;
    logger.log_level := 'ALL';

    logger.info(l_proc_name || ' Start');

    EXECUTE IMMEDIATE 'truncate table Z_PCDW_FRU_OPENPO';

    EXECUTE IMMEDIATE 'truncate table Z_MID_FRU_OPENPO';

    INSERT INTO z_mid_fru_openpo
      (ebeln, ebelp, matnr, werks, bsart, aedat_h, loekz_h, lifnr, frgke, frgrl, kdatb, kdate, reswk,
       zzmatsrc, aedat_l, loekz_l, menge, knttp, elikz, lgort, reslo, retpo, pstyp, zzodmfd, lprio, vbeln,
       vbelp, veten, openqty, sys_ent_state, sys_created_by, sys_creation_date, sys_last_modified_by,
       sys_last_modified_date, ihrez, newdate, vsbed, banfn, bnfpo, bednr, tex)
      SELECT ebeln, ebelp, matnr, werks, bsart, aedat_h, loekz_h, lifnr, frgke, frgrl, kdatb, kdate, reswk,
             zzmatsrc, aedat_l, loekz_l, menge, knttp, elikz, lgort, reslo, retpo, pstyp, zzodmfd, lprio,
             vbeln, vbelp, veten,
             --PO qty - TOTAL GR Qty
             nvl(a.menge, 0) - nvl((SELECT SUM(wemng)
                                      FROM prdpcdw.pcdw_po_sl b
                                     WHERE a.ebeln = b.ebeln
                                       AND a.ebelp = b.ebelp), 0) AS openqty, sys_ent_state,
             l_proc_name AS sys_created_by, SYSDATE AS sys_creation_date, l_proc_name AS sys_last_modified_by,
             SYSDATE AS sys_last_modified_date, ihrez, newdate, vsbed, banfn, bnfpo, bednr, tex
        FROM prdpcdw.pcdw_po a
       WHERE a.werks IN ('H021', 'H022', 'H023')
         AND a.aedat_h > trunc(SYSDATE) - 365
         AND a.loekz_l IS NULL
         AND a.loekz_h IS NULL;

    COMMIT;

    --init data Z_PCDW_FRU_OPENPO from Z_MID_FRU_PO_INIT
    INSERT INTO z_pcdw_fru_openpo
      (ebeln, ebelp, matnr, werks, aedat_h, lifnr, menge, openqty, sys_creation_date, tex)
      SELECT po_id, nvl(po_line_id,'00010'), item, siteid, po_creation_date, supplierid, po_qty,
             nvl(a.po_qty, 0) - nvl((SELECT SUM(wemng)
                                      FROM prdpcdw.pcdw_po_sl b
                                     WHERE a.po_id = b.ebeln
                                       AND nvl(po_line_id,'00010') = b.ebelp), 0) AS openqty,
             sys_creation_date, remarks
        FROM z_mid_fru_po_init a ;

    COMMIT;

    MERGE INTO z_pcdw_fru_openpo a
    USING z_mid_fru_openpo b
    ON (a.ebeln = b.ebeln AND a.matnr = b.matnr)
    WHEN MATCHED THEN
      UPDATE SET a.openqty = b.openqty
    WHEN NOT MATCHED THEN
      INSERT
        (a.ebeln, a.ebelp, a.matnr, a.werks, a.aedat_h, a.lifnr, a.menge, a.openqty, a.sys_creation_date,
         a.tex)
      VALUES
        (b.ebeln, b.ebelp, b.matnr, b.werks, b.aedat_h, b.lifnr, b.menge, b.openqty, SYSDATE, b.tex);
    COMMIT;

    --check the remark logic,
    --delete rebalance order
    DELETE FROM z_pcdw_fru_openpo WHERE lifnr = '0004007310'; --upper(tex) like '%REBALANCE%'; -- only wistron has rebalance po

    COMMIT;
    --end

    DELETE FROM z_pcdw_fru_openpo WHERE openqty <= 0;
    DELETE FROM z_pcdw_fru_openpo
     WHERE lifnr IN (SELECT pattribute
                       FROM z_ui_conf_parameter
                      WHERE pdomain = 'ODM_CODE'
                        AND pvalue IN (/*'LCFC', */'WISTRON')); ---remove the LCFC by xuml7, because LCFC not send the fru info by FTP 20240318

    COMMIT;

    --need mapping the odm to engine

    IF to_number(to_char(SYSDATE, 'D', 'NLS_DATE_LANGUAGE = American')) = v_snapdate
    THEN
      EXECUTE IMMEDIATE 'ALTER TABLE Z_PCDW_FRU_OPENPO_HIS TRUNCATE SUBPARTITION P_' ||
                        to_char(v_currentweek, 'YYYY') || '_W_' || to_char(v_currentweek, 'WW');

      INSERT INTO z_pcdw_fru_openpo_his
        (versionyear, versionweek, versiondate, ebeln, ebelp, matnr, werks, bsart, aedat_h, loekz_h, lifnr,
         frgke, frgrl, kdatb, kdate, reswk, zzmatsrc, aedat_l, loekz_l, menge, knttp, elikz, lgort, reslo,
         retpo, pstyp, zzodmfd, lprio, vbeln, vbelp, veten, openqty, sys_ent_state, sys_created_by,
         sys_creation_date, sys_last_modified_by, sys_last_modified_date, ihrez, newdate, vsbed, banfn, bnfpo,
         bednr)
        SELECT trunc(v_currentweek, 'yyyy'), to_char(v_currentweek, 'ww'), v_currentweek, ebeln, ebelp, matnr,
               werks, bsart, aedat_h, loekz_h, lifnr, frgke, frgrl, kdatb, kdate, reswk, zzmatsrc, aedat_l,
               loekz_l, menge, knttp, elikz, lgort, reslo, retpo, pstyp, zzodmfd, lprio, vbeln, vbelp, veten,
               openqty, sys_ent_state, sys_created_by, sys_creation_date, sys_last_modified_by,
               sys_last_modified_date, ihrez, newdate, vsbed, banfn, bnfpo, bednr
          FROM z_pcdw_fru_openpo;

      COMMIT;
    END IF;

    logger.info(l_proc_name || ' Completed');

    exitcode := 0;
  EXCEPTION
    WHEN OTHERS THEN
      exitcode := SQLCODE;
      logger.error;
      RAISE;
  END;

  PROCEDURE prc_odm_resv_demand(wfl_id VARCHAR2, node_id VARCHAR2, iv_id VARCHAR2, exitcode OUT NUMBER) AS
    l_proc_name VARCHAR2(40) := g_logic_name || '.prc_odm_resv_demand';
  BEGIN
    exitcode         := -20099;
    logger.wfl_id    := wfl_id;
    logger.node_id   := node_id;
    logger.log_level := 'ALL';

    logger.info(l_proc_name || ' Start');

    EXECUTE IMMEDIATE 'truncate table z_pcdw_resv_demand';

    MERGE INTO z_pcdw_resv_demand a
    USING (SELECT odm, nvl(lenovopn, odmpn) AS lenovopn, SUM(nvl(inv_qty, 0)) AS inv_qty
             FROM prdpcdw.imp_odm_inventory
            GROUP BY odm, nvl(lenovopn, odmpn)) b
    ON (a.odm = b.odm AND a.lenovopn = b.lenovopn)
    WHEN MATCHED THEN
      UPDATE SET a.inv_qty = b.inv_qty
    WHEN NOT MATCHED THEN
      INSERT
        (odm, lenovopn, inv_qty, resv_qty, demand_qty, sys_creation_date, sys_created_by)
      VALUES
        (b.odm, b.lenovopn, b.inv_qty, 0, 0, SYSDATE, l_proc_name);

    COMMIT;

    MERGE INTO z_pcdw_resv_demand a
    USING (SELECT odm, nvl(lenovo_item, odm_item) AS lenovopn, SUM(nvl(res_qty, 0)) AS res_qty
             FROM prdpcdw.imp_odm_mo_reservation
            GROUP BY odm, nvl(lenovo_item, odm_item)) b
    ON (a.odm = b.odm AND a.lenovopn = b.lenovopn)
    WHEN MATCHED THEN
      UPDATE SET a.resv_qty = b.res_qty
    WHEN NOT MATCHED THEN
      INSERT
        (odm, lenovopn, inv_qty, resv_qty, demand_qty, sys_creation_date, sys_created_by)
      VALUES
        (b.odm, b.lenovopn, 0, b.res_qty, 0, SYSDATE, l_proc_name);

    COMMIT;

    UPDATE z_pcdw_resv_demand SET demand_qty = resv_qty - inv_qty;

    COMMIT;

    --need get demand_qty >0 rows to engine

    logger.info(l_proc_name || ' Completed');

    exitcode := 0;
  EXCEPTION
    WHEN OTHERS THEN
      exitcode := SQLCODE;
      logger.error;
      RAISE;
  END;

  PROCEDURE prc_odm_part(wfl_id VARCHAR2, node_id VARCHAR2, iv_id VARCHAR2, exitcode OUT NUMBER) AS
    l_proc_name VARCHAR2(40) := g_logic_name || '.prc_odm_part';
  BEGIN
    exitcode         := -20099;
    logger.wfl_id    := wfl_id;
    logger.node_id   := node_id;
    logger.log_level := 'ALL';

    logger.info(l_proc_name || ' Start');

    EXECUTE IMMEDIATE 'truncate table Z_MID_ODM_PART';

    INSERT INTO z_mid_odm_part
      (odm, odm_item)
      SELECT DISTINCT upper(odm), father_odmpn FROM prdpcdw.imp_odm_bom;

    COMMIT;

    --need get demand_qty >0 rows to engine

    logger.info(l_proc_name || ' Completed');

    exitcode := 0;
  EXCEPTION
    WHEN OTHERS THEN
      exitcode := SQLCODE;
      logger.error;
      RAISE;
  END;

  PROCEDURE prc_odm_sfg_gr(wfl_id VARCHAR2, node_id VARCHAR2, iv_id VARCHAR2, exitcode OUT NUMBER) AS
    l_proc_name VARCHAR2(40) := g_logic_name || '.prc_odm_sfg_gr';
  BEGIN
    exitcode         := -20099;
    logger.wfl_id    := wfl_id;
    logger.node_id   := node_id;
    logger.log_level := 'ALL';

    logger.info(l_proc_name || ' Start');

    EXECUTE IMMEDIATE 'truncate table z_pcdw_odm_sfg_gr';

    INSERT INTO z_pcdw_odm_sfg_gr
      (odm, odmpn, lenovopn, received_qty, received_date, received_from, sys_created_by, sys_created_date)
      SELECT odm, odmpn, lenovopn, received_qty, received_date, received_from, sys_created_by,
             sys_created_date
        FROM prdpcdw.imp_odm_sfg_gr
        where  nvl(flag,0)=0;

    COMMIT;

    logger.info(l_proc_name || ' Step 2');
    prc_touch_simulation(wfl_id, node_id, iv_id, exitcode);

    logger.info(l_proc_name || ' Step 3');


    if to_char(sysdate,'d') = 5 then
    prc_mex_intransit(wfl_id, node_id, iv_id, exitcode);
    prc_odm_intransit_LCFC(wfl_id,node_id,iv_id,exitcode);
    end if;

    logger.info(l_proc_name || ' Completed');



    exitcode := 0;
  EXCEPTION
    WHEN OTHERS THEN
      exitcode := SQLCODE;
      logger.error;
      RAISE;
  END;
  PROCEDURE prc_touch_simulation(wfl_id VARCHAR2, node_id VARCHAR2, iv_id VARCHAR2, exitcode OUT NUMBER) AS

    l_proc_name VARCHAR2(40) := 'PRC_TOUCH_SIMULATION';
    l_appid     VARCHAR2(40) := 'TOUCH_SIMULATION';
    l_instid    VARCHAR2(40) := 'TOUCH_SIMULATION';
    g_end       DATE;
    g_start     DATE;
  BEGIN
    exitcode := -20099;

    pkg_base_funcs.init_log_run(l_proc_name || ':' || iv_id);
    g_end := SYSDATE;
    logger.info(l_proc_name || ' Start');

    SELECT pkg_net_change.func_get_last_runtime(wfl_id, node_id, l_appid, l_instid) INTO g_start FROM dual;

    --lcd
    logger.info(l_proc_name || ' step1');

    --'YSSF' is the BS VMI auto SO type
    merge INTO z_fact_lcd_ship a
    using (SELECT a.werks,b.vbeln, b.posnr, b.vbgel, b.matnr, b.vgpos, a.kwmeng,
             decode(b.vbtyp, 'T', -b.lfimg, b.lfimg) lfimg,
             --??
             a.auart, b.wadat_ist, a.sold2_kunnr, b.sys_ent_state
        FROM pcdw_so a
       INNER JOIN pcdw_delivery b
          ON a.vbeln = b.vbgel
         AND a.posnr = b. vgpos
         --AND a.auart IN ('YSSB', 'YSSC','YSSF')
         AND a.auart IN ('YSSA', 'YSSB','YSSF')--add by xuml7 20200409 for lcd ship order type
         AND a.werks in( 'H000','H121','H112')--h000:think,H121 LNB
         AND b. vbtyp IN ('J', 'T') --J ???,T ???,?????
         AND b. sys_last_modified_date BETWEEN g_start AND g_end
       INNER JOIN z_dim_component_bs c
          ON a.matnr = c.component
         AND c.hier5_code = 'DISPLAY') b
    on (a.deliveryno = b.vbeln
    and a.deliveryline = b.posnr)
    when matched then
        update
        set a.sys_ent_state = b.sys_ent_state,
            a.werks = b.werks,
            a.shipdate = b.wadat_ist,
            a.shipqty = b.lfimg
    when not matched then
       insert
          (a.werks,a.deliveryno, a.deliveryline, a.lineid, a.item, a.orderid,
           a.orderqty, a.shipqty, a.ordertype, a.shipdate, a.soldto,
           a.sys_ent_state, a.sys_created_by, a.sys_modified_by)
       values
          (b.werks,b.vbeln,b.posnr,b.vbgel, b.matnr, b.vgpos,b.kwmeng,
           b.lfimg,b.auart, b.wadat_ist, b.sold2_kunnr, b.sys_ent_state,
           l_proc_name, l_proc_name)
      ;

    COMMIT;

    --TOUCH
    logger.info(l_proc_name || ' step2');

    merge INTO z_fact_touch_ship a
    using (SELECT a.werks,b.vbeln, b.posnr, b.vbgel, b.matnr, b.vgpos, a.kwmeng, decode(b.vbtyp, 'T', -b.lfimg, b.lfimg) lfimg,
             a.auart, b.wadat_ist, a.sold2_kunnr, b.sys_ent_state
        FROM pcdw_so a
       INNER JOIN pcdw_delivery b
          ON a.vbeln = b.vbgel
         AND a.posnr = b. vgpos
         AND a.auart in( 'YSSA','YSSF')
         AND a.werks in( 'H000','H121','H112')----h000:think,H121 LNB
         AND b. vbtyp IN ('J', 'T') --J ???,T ???,?????
         AND b. sys_last_modified_date BETWEEN g_start AND g_end
       INNER JOIN z_dim_component_bs c
          ON a.matnr = c.component
         AND c.hier5_code IN ('TOUCH', 'TOUCHPANEL')
         ) b
    on (a.deliveryno = b.vbeln
    and a.deliveryline = b.posnr)
    when matched then
        update
        set a.sys_ent_state = b.sys_ent_state,
            a.werks = b.werks,
            a.shipdate = b.wadat_ist,
            a.shipqty = b.lfimg
    when not matched then
       insert
          (a.werks,a.deliveryno, a.deliveryline, a.lineid, a.item, a.orderid,
           a.orderqty, a.shipqty, a.ordertype, a.shipdate, a.soldto,
           a.sys_ent_state, a.sys_created_by, a.sys_modified_by)
       values
          (b.werks,b.vbeln,b.posnr,b.vbgel, b.matnr, b.vgpos,b.kwmeng,
           b.lfimg,b.auart, b.wadat_ist, b.sold2_kunnr, b.sys_ent_state,
           l_proc_name, l_proc_name)
      ;

    COMMIT;

    -- Update Net Change Time.
    pkg_net_change.prc_update_runtime(wfl_id, node_id, l_appid, l_instid, g_end, 'success', exitcode);

    logger.info(l_proc_name || ' Completed');
    pkg_base_funcs.update_log_run(l_proc_name || ':' || iv_id, 0);
    exitcode := 0;

  EXCEPTION
    WHEN OTHERS THEN
      pkg_net_change.prc_update_runtime(wfl_id, node_id, l_appid, l_instid, g_end, 'aborted', exitcode);
      exitcode := SQLCODE;
      logger.error;
      pkg_base_funcs.update_log_run(l_proc_name || ':' || iv_id, -1);
      RAISE;

  END prc_touch_simulation;

  procedure prc_mex_intransit(wfl_id   varchar2,
                                node_id  varchar2,
                                iv_id    varchar2,
                                exitcode out number) as
      l_proc_name varchar2(100) := g_logic_name || '.PRC_MEX_INTRANSIT';
      /***************************************************************
      --author--   --date---     ---event------
      yanghuaping   2019-09-05
      ***************************************************************/
    begin

      logger.info(l_proc_name || ' Start');

     --- back up data add by lucq1 20191206
   DELETE FROM r3_intransitshpmnt_snap_bak WHERE baktime < trunc(SYSDATE) - 90;
   INSERT INTO r3_intransitshpmnt_snap_bak
     (scenario_id, shipmentid, item, tositeid, scheduleddedate, shipmentqty, fromsiteid, dept, sys_target_id,
      sys_auth_id, sys_source, sys_created_by, sys_creation_date, sys_ent_state, sys_last_modified_by,
      sys_last_modified_date, sys_nc_type, sys_err_code, sys_err_svrty, sys_filter, sys_exception_type,
      intransitqty, receiptqty, TIMESTAMP, baktime,BU)
     SELECT scenario_id, shipmentid, item, tositeid, scheduleddedate, shipmentqty, fromsiteid, dept,
            sys_target_id, sys_auth_id, sys_source, sys_created_by, sys_creation_date, sys_ent_state,
            sys_last_modified_by, sys_last_modified_date, sys_nc_type, sys_err_code, sys_err_svrty, sys_filter,
            sys_exception_type, intransitqty, receiptqty, TIMESTAMP, SYSDATE,BU
       FROM prdabppmexnb.r3_intransitshpmnt_snap;
   COMMIT;
   DELETE FROM z_mid_intransit_bak WHERE baktime < trunc(SYSDATE) - 90;
   DELETE FROM z_mid_mex_lipc_poso_list_bak WHERE baktime < trunc(SYSDATE) - 90;
   DELETE FROM z_mid_mex_lipc_ship_list_bak WHERE baktime < trunc(SYSDATE) - 90;
   INSERT INTO z_mid_intransit_bak
     (item, po, fromid, tositeid, intransitqty, poline, sys_created_date, baktime,BU)
     SELECT item, po, fromid, tositeid, intransitqty, poline, sys_created_date, SYSDATE,BU FROM z_mid_intransit;

   INSERT INTO z_mid_mex_lipc_poso_list_bak
     (po, orderid, orderline, matnr, kwmeng, werks, erdat, auart, sys_creaeted_date, baktime)
     SELECT po, orderid, orderline, matnr, kwmeng, werks, erdat, auart, sys_creaeted_date, SYSDATE
       FROM z_mid_mex_lipc_poso_list;
   INSERT INTO z_mid_mex_lipc_ship_list_bak
     (vbeln, posnr, orderid, orderline, matnr, werks, gi, lgort, erdat, erzet, sys_created_date, po, baktime)
     SELECT vbeln, posnr, orderid, orderline, matnr, werks, gi, lgort, erdat, erzet, sys_created_date, po,
            SYSDATE
       FROM z_mid_mex_lipc_ship_list;
   COMMIT;
   EXECUTE IMMEDIATE 'truncate table z_mid_intransit';
   EXECUTE IMMEDIATE 'truncate table z_mid_mex_lipc_poso_list';
   EXECUTE IMMEDIATE 'truncate table z_mid_mex_lipc_ship_list';

--1. ODM intransit = shipment - gr
      /*INSERT INTO z_mid_intransit
        (po, poline, item, fromid, tositeid, intransitqty,BU)
        SELECT m.po,
               prdpcdw.lpadnum(m.lineid, 5, 1) AS poline,
               m.material,
               m.odm fromid,
               'X420' tositeid,
               m.ship_qty - nvl(m2.gr, 0) intransitqty,M.BU
          FROM (SELECT po, lineid, material, odm, SUM(ship_qty) ship_qty,BU
                   FROM prdpcdw.imp_odm_po_shipment
                  GROUP BY po, lineid, material, odm,BU) m
        \*inner join (select ponumber, item, matnr, sum(openquan) openquan, max(POQUAN) as ttl_qty
         from prdspmexnb.r3_openpo_snap
        where createdate >= trunc(sysdate) - 180
        group by ponumber,item, matnr) m1*\
        \* LEFT JOIN (SELECT ebeln AS ponumber, ebelp AS item, matnr, werks, lgort, SUM(menge) AS gr
                       FROM prdpcdw.pcdw_mat_doc PARTITION (p_2019)
                      WHERE bwart = '101'
                        AND trunc(budat) < prdpcgsp.get_curr_version('ThinkNB')+3
                        AND werks = 'X420'
                      GROUP BY ebeln, ebelp, matnr, werks, lgort) m2*\
             LEFT JOIN (SELECT ebeln AS ponumber, ebelp AS item, matnr, werks, \*lgort,*\--comment lgort by qinying4 20200907
                       SUM(decode(bwart, '101', menge, '102', -menge)) AS gr
                       FROM prdpcdw.pcdw_mat_doc --PARTITION (p_2019)
                      WHERE bwart in ( '101','102')
                        AND trunc(budat) < prdpcgsp.get_curr_version('ThinkNB')+3
                        AND werks = 'X420'
                        AND CPUDT>=TO_DATE('20190101','YYYY/MM/DD') --by lucq1 20191231
                      GROUP BY ebeln, ebelp, matnr, werks\*, lgort*\) m2 --by lucq1 20191213
            ON m.po = m2.ponumber
           AND prdpcdw.lpadnum(m.lineid, 5, 1) = m2.item;

      commit;*/

      INSERT INTO z_mid_intransit
        (po, poline, item, fromid, tositeid, intransitqty,BU)
        SELECT m.po,
               prdpcdw.lpadnum(m.lineid, 5, 1) AS poline,
               m.material,
               m.odm fromid,
               'X220' tositeid,
               m.ship_qty - nvl(m2.gr, 0) intransitqty,M.BU
          FROM (SELECT po, lineid, material, odm, SUM(ship_qty) ship_qty,BU
                   FROM prdpcdw.imp_odm_po_shipment a
                where exists(select 1 from prdpcdw.ecc_po_n@prdmdmn_19c_new b where a.po = b.ebeln and prdpcdw.lpadnum(a.lineid, 5, 1) = b.ebelp and b.werks = 'X220')
                  GROUP BY po, lineid, material, odm,BU) m
             LEFT JOIN (SELECT ebeln AS ponumber, ebelp AS item, matnr, werks, /*lgort,*/--comment lgort by qinying4 20200907
                       SUM(decode(bwart, '101', menge, '102', -menge)) AS gr
                       FROM prdpcdw.pcdw_mat_doc@prdmdmn_19c_new
                      WHERE bwart in ( '101','102')
                        AND trunc(budat) < prdpcgsp.get_curr_version('ThinkNB')+3
                        AND werks = 'X220'
                        AND CPUDT>=TO_DATE('20240101','YYYY/MM/DD') --by lucq1 20191231
                      GROUP BY ebeln, ebelp, matnr, werks/*, lgort*/) m2 --by lucq1 20191213
            ON m.po = m2.ponumber
           AND prdpcdw.lpadnum(m.lineid, 5, 1) = m2.item;
      commit;

      UPDATE z_mid_intransit SET intransitqty = 0 WHERE intransitqty<0;
      COMMIT;


      DELETE FROM prdabppmexnb.r3_intransitshpmnt_snap WHERE SYS_SOURCE = 'PRDSPOEMTHK';

      /*INSERT INTO prdabppmexnb.r3_intransitshpmnt_snap
        (shipmentid, item, tositeid, fromsiteid, shipmentqty, scheduleddedate, intransitqty, receiptqty,
         TIMESTAMP, SYS_SOURCE,BU)
        SELECT po || poline, item, tositeid, fromid, intransitqty, trunc(SYSDATE) AS scheduleddedate, intransitqty,
               0, SYSDATE, 'PRDSPOEMTHK' AS SYS_SOURCE,BU
          FROM prdspoemthk.z_mid_intransit;*/

      DELETE FROM prdmrpnb.mty_intransitshpmnt_snap@prdmdmn_19c_new WHERE SYS_SOURCE = 'PRDSPOEMTHK';

      INSERT INTO prdmrpnb.mty_intransitshpmnt_snap@prdmdmn_19c_new
        (shipmentid, item, tositeid, fromsiteid, shipmentqty, scheduleddedate, intransitqty, receiptqty,
         TIMESTAMP, SYS_SOURCE,BU)
        SELECT po || poline, item, tositeid, fromid, intransitqty, trunc(SYSDATE) AS scheduleddedate, intransitqty,
               0, SYSDATE, 'PRDSPOEMTHK' AS SYS_SOURCE,BU
          FROM prdspoemthk.z_mid_intransit;
      commit;

--2. LIPC to MTY intransit 20191113
     /*DELETE FROM z_mid_mex_lipc_poso_list;
     DELETE FROM z_mid_mex_lipc_ship_list; */

     --Get so
     INSERT INTO z_mid_mex_lipc_poso_list
       (po, orderid, orderline, matnr, kwmeng, werks, erdat, auart, sys_creaeted_date)
       SELECT /*+ use_hash(a,b)*/bstkd AS po, vbeln AS orderid, posnr AS orderline, matnr, kwmeng, werks, erdat, auart,
              SYSDATE AS sys_creaeted_date
         FROM prdpcdw.pcdw_so a --PARTITION (p_2019) a
        WHERE EXISTS (SELECT 1
                 FROM prdpcdw.pcdw_po b --PARTITION (p_2019) b
                WHERE --a.bstkd = b.ebeln --Modified by qinying4 for blanket PO
                      --instr(a.bstkd,b.ebeln)>0
                      a.bstkd = b.ebeln --edit by xuml7 20230801
                  --AND a.matnr = b.matnr
                  AND werks = 'X420'
                  AND bsart = 'ZIMB'
                  AND lifnr = '0001000210'
                  AND NVL(LOEKZ_L,' ')<>'L'
                  AND AEDAT_H>=TO_DATE('20190101','YYYY/MM/DD')) --by lucq1 20191231
          AND l_abgru IS NULL
          AND werks = 'L420'
          AND ERDAT>=TO_DATE('20190101','YYYY/MM/DD') --by lucq1 20191231
          ;
   --Get so for L220 add by huangpz2 20230207
    INSERT INTO z_mid_mex_lipc_poso_list
    (po, orderid, orderline, matnr, kwmeng, werks, erdat, auart, sys_creaeted_date)
     SELECT /*+ use_hash(a,b)*/CUSTOMER_PURCHASE_NUM AS po, vbeln AS orderid, posnr AS orderline, matnr, kwmeng, werks, erdat, auart,
     SYSDATE AS sys_creaeted_date
     FROM prdpcdw.ecc_so_n@prdmdmn_19c_lk a --PARTITION (p_2019) a
     WHERE EXISTS (SELECT 1
     FROM prdpcdw.pcdw_po b --PARTITION (p_2019) b
     WHERE a.CUSTOMER_PURCHASE_NUM = b.ebeln
     --instr(a.bstkd,b.ebeln)>0
     --AND a.matnr = b.matnr
     and werks = 'X420'
     AND bsart in ('ZNP','ZIMB')
     AND lifnr in ('0001000835','0001000210')--add by huangpz2 20230314 for intransit request
     AND NVL(LOEKZ_L,' ')<>'L'
     AND AEDAT_H>=TO_DATE('20190101','YYYY/MM/DD')) --by lucq1 20191231
     AND l_abgru IS NULL
     AND werks = 'L220'
     AND ERDAT>=TO_DATE('20190101','YYYY/MM/DD') --by lucq1 20191231
     ;
     --Get shipment
     /*INSERT INTO z_mid_mex_lipc_ship_list
       (vbeln, posnr, orderid, orderline, po, matnr, werks, gi, lgort, erdat, erzet, sys_created_date)
       SELECT a.vbeln, a.posnr, a.vbelv AS orderid, a.vgpos AS orderline, b.po, a.matnr, a.werks, a.lfimg AS gi, a.lgort, a.erdat,
              a.erzet, SYSDATE AS sys_created_date
         FROM prdpcdw.pcdw_delivery a --PARTITION (p_2019) a
         join z_mid_mex_lipc_poso_list b
         on  a.vbelv = b.orderid
        WHERE a.werks = 'L420'
          AND trunc(WADAT_IST) < prdpcgsp.get_curr_version('ThinkNB')+3
          AND a.sys_ent_state = 'ACTIVE'
          AND a.ERDAT>=TO_DATE('20190101','YYYY/MM/DD') --by lucq1 20191231
          ;*/
     --Get shipment for L220 add by huangpz2 20230207
         INSERT INTO z_mid_mex_lipc_ship_list
         (vbeln, posnr, orderid, orderline, po, matnr, werks, gi, lgort, erdat, erzet, sys_created_date)
          SELECT /*+ leading(b) use_hash(a,b)*/ a.vbeln, a.posnr, a.VGBEL AS orderid, a.vgpos AS orderline, b.po, a.matnr, a.werks, a.lfimg AS gi, a.lgort, a.erdat,
          a.erzet, SYSDATE AS sys_created_date
         FROM prdpcdw.ecc_delivery@prdmdmn_19c_lk a --PARTITION (p_2019) a
         join z_mid_mex_lipc_poso_list b
         on a.VGBEL = b.orderid
         WHERE a.werks = 'L220'
         AND trunc(WADAT_IST) < prdpcgsp.get_curr_version('ThinkNB')+3
         --- AND a.sys_ent_state = 'ACTIVE'
         AND a.ERDAT>=TO_DATE('20190101','YYYY/MM/DD') --by lucq1 20191231
         ;
     --DENGRL ADD as Linda Li request these two order delivery lost, should be deleted in calculate intransit.
     delete FROM z_mid_mex_lipc_ship_list A
     WHERE  (ORDERID='4119236625' AND VBELN='8985836975') OR (ORDERID='4119236627' AND VBELN='8985836976');
     commit;
     --end DENGRL ADD as Linda Li request 2020.12.8

     --Calculate intransit
     INSERT INTO prdabppmexnb.r3_intransitshpmnt_snap
       (shipmentid, item, tositeid, fromsiteid, shipmentqty, scheduleddedate, intransitqty, receiptqty, TIMESTAMP,
        sys_source,BU)
       SELECT a.po, a.matnr, nvl(b.werks, 'X420') AS tositeid, a.werks AS fromsiteid,
              SUM(nvl(a.gi, 0)) AS shipmentqty, trunc(SYSDATE) AS scheduleddedate,
              SUM(nvl(a.gi, 0)) - SUM(nvl(b.gr, 0)) AS intransit, SUM(nvl(b.gr, 0)) AS receiptqty, SYSDATE,
              'PRDSPOEMTHK' AS sys_source,'ThinkNB' BU
         FROM (SELECT substr(po,1,10) as po,  --2020.12.8:dengrl add substr as po column has PO number + SO GI DATE
                      --decode(substr(AA.matnr, 1, 4), 'SEM0', 'SS50'||substr(AA.matnr,5),AA.matnr) AS matnr,
                      nvl(bb.dummyshell,aa.matnr) AS matnr,--Modified by qinying4
                      werks, SUM(gi) AS gi
                  FROM z_mid_mex_lipc_ship_list AA
                  left join prdmrpnb.z_dim_shell_mapping@prdmdmn_19c_new BB
                   ON AA.matnr = BB.purchaseshell
                 GROUP BY substr(po,1,10), matnr, werks, nvl(bb.dummyshell,aa.matnr)) a
        /* LEFT JOIN (SELECT ebeln, ebelp, matnr, werks, lgort, SUM(menge) AS gr
                      FROM prdpcdw.pcdw_mat_doc PARTITION (p_2019)
                     WHERE bwart = '101'
                       AND trunc(budat) < prdpcgsp.get_curr_version('ThinkNB')+3
                       AND lifnr = '0001000210'
                       AND werks = 'X420'
                     GROUP BY ebeln, ebelp, matnr, werks, lgort) b*/
          LEFT JOIN (SELECT ebeln, ebelp, matnr, werks, lgort,
                      SUM(decode(bwart, '101', menge, '102', -menge)) AS gr
                      FROM prdpcdw.pcdw_mat_doc --PARTITION (p_2019)
                     WHERE bwart in ( '101','102')
                       AND trunc(budat) < prdpcgsp.get_curr_version('ThinkNB')+3
                       AND lifnr in('0001000210','0001000835')---add by huangpz2 for 0001000835 L220 20230207
                       AND werks = 'X420'
                       AND CPUDT>=TO_DATE('20190101','YYYY/MM/DD') --by lucq1 20191231
                     GROUP BY ebeln, ebelp, matnr, werks, lgort) b --by lucq1 20191213
           ON a.po = b.ebeln
        GROUP BY a.po, a.matnr, a.werks, b.werks
        --HAVING SUM(nvl(a.gi, 0)) - SUM(nvl(b.gr, 0)) > 0
        ;

     delete from  prdabppmexnb.r3_intransitshpmnt_snap where INTRANSITQTY = 0;

     commit;
      logger.info(l_proc_name || ' Completed');
      exitcode := 0;

    exception
      when others then
        exitcode := sqlcode;
        logger.error;
        raise;
    end;


  PROCEDURE prc_odm_bom_tst(wfl_id VARCHAR2, node_id VARCHAR2, iv_id VARCHAR2, exitcode OUT NUMBER) AS
    l_proc_name   VARCHAR2(40) := g_logic_name || '.PRC_ODM_BOM';
    l_crr_version DATE := get_curr_version('BSMRP');
  BEGIN
    exitcode         := -20099;
    logger.wfl_id    := wfl_id;
    logger.node_id   := node_id;
    logger.log_level := 'ALL';

    pkg_base_funcs.init_log_run(g_logic_name || ':' || iv_id);

    logger.info(l_proc_name || ' Start');

    EXECUTE IMMEDIATE 'truncate table z_mid_imp_odm_bom';
    EXECUTE IMMEDIATE 'truncate table z_pcdw_odm_bom';

    DELETE FROM imp_odm_bom WHERE son_lenovopn = '41U4034';

    delete from imp_odm_bom
    where father_lenovopn  in ('SBB0Q20867','SBB0Q17719')
    and son_odmpn in ('2A585K700-G98-G')
    and alpgr not in(1,7)
    and odm='FOXCONN'  ;

    COMMIT;

    UPDATE imp_odm_bom
    SET SON_LENOVOPN=''
    WHERE NVL(SON_LENOVOPN,' ') IN('SM30U64434','SM30U64433')
    AND ODM='LCFC';

    UPDATE imp_odm_bom
    SET FATHER_LENOVOPN=''
    WHERE NVL(FATHER_LENOVOPN,' ') IN('SM30U64434','SM30U64433')
    AND ODM='LCFC';
    COMMIT;

    INSERT INTO z_pcdw_odm_bom
      (odm, father_lenovopn, father_odmpn, son_lenovopn, son_odmpn, qtyper, alpgr, alprf, effstartdate,
       effenddate, odm_created_date, sys_created_by, sys_created_date)
      SELECT /*+ parallel(a 4)*/
      DISTINCT DECODE(upper(odm),'FOXCONNL6','FOXCONN',odm), TRIM(father_lenovopn), TRIM(father_odmpn), TRIM(son_lenovopn), TRIM(son_odmpn),
               qtyper, alpgr, alprf,
               case when trunc(sys_created_date) = trunc(effstartdate) then trunc(effstartdate,'IW')
                      else effstartdate
               end as effstartdate, --add by zy3
               effenddate, odm_created_date, g_logic_name, SYSDATE
        FROM prdpcdw.imp_odm_bom a
       WHERE instr(son_lenovopn, '_') = 0
          OR son_lenovopn IS NULL;
    COMMIT;

    --Add by qinying4 tempory start
    UPDATE Z_PCDW_ODM_BOM
    SET SON_LENOVOPN=''
    WHERE ODM='LCFC'
    AND SON_LENOVOPN='SP10K38391'
    AND FATHER_LENOVOPN IN('SSA0S71773','SSA0S71774','SSA0T37581','SW10A11646','SW10K97446');
    COMMIT;
    --Add by qinying4 tempory end

    --need split son_lenovopn
    INSERT INTO z_mid_imp_odm_bom
      (odm, father_lenovopn, father_odmpn, son_lenovopn, son_odmpn, qtyper, alpgr, alprf, effstartdate,
       effenddate, odm_created_date, sys_created_by, sys_created_date)
      SELECT /*+ parallel(a 4)*/
       DECODE(upper(odm),'FOXCONNL6','FOXCONN',odm), TRIM(father_lenovopn), TRIM(father_odmpn), TRIM(son_lenovopn), TRIM(son_odmpn), qtyper,
       alpgr, alprf, effstartdate, effenddate, odm_created_date, sys_created_by, sys_created_date
        FROM prdpcdw.imp_odm_bom a
       WHERE instr(son_lenovopn, '_') > 0;
    COMMIT;
    --split son_lenovopn
    INSERT INTO z_pcdw_odm_bom
      (odm, father_lenovopn, father_odmpn, son_lenovopn, son_odmpn, qtyper, alpgr, alprf, effstartdate,
       effenddate, odm_created_date, sys_created_by, sys_created_date)
      SELECT DISTINCT odm, father_lenovopn, father_odmpn,
                      regexp_substr(son_lenovopn, '[^_]+', 1, rn) son_lenovopn, son_odmpn, qtyper, alpgr,
                      alprf, effstartdate, effenddate, odm_created_date, g_logic_name, SYSDATE
        FROM z_mid_imp_odm_bom a, (SELECT LEVEL rn FROM dual CONNECT BY LEVEL <= 40) b --add by qinying for LCFC multi-son_lenovopn
       WHERE rn <= regexp_count(nvl(son_lenovopn, 'X'), '[^_]+');

    COMMIT;

    --delete lcfc mtm -sbb bom

    DELETE FROM z_pcdw_odm_bom a
     WHERE EXISTS (SELECT 1
              FROM z_dim_sbb_bs c
             WHERE a.son_lenovopn = c.sbb
               AND a.odm = 'LCFC' -- add by zy3, LITEON MTM BOM need ADD SBB son part
               )
           --Add by qinying for MTY shell
           and not exists(
                  select 1 from z_dim_component_bs b
                  where a.father_lenovopn=b.component
                  and b.hier2_code in('SHELL','BS_GBM'))
             ---add by xujc1 exclude LCFC MTM-SBB BOM
    ;

  --delete lcfc ISSUE BOM 20181101
  delete from z_pcdw_odm_bom WHERE FATHER_LENOVOPN IN ('5SB0N24830') and odm ='LCFC';--,'5SB0N24828');

    COMMIT;

    logger.info(l_proc_name || ' Completed');
      exitcode := 0;

    exception
      when others then
        exitcode := sqlcode;
        logger.error;
        raise;
    end;
  PROCEDURE prc_odm_intransit_LCFC(wfl_id   VARCHAR2,
                              node_id  VARCHAR2,
                              iv_id    varchar2,
                              exitcode OUT NUMBER) AS
    l_proc_name              VARCHAR2(40) := g_logic_name ||
                                             '.PRC_ODM_INTRANSIT_LCFC';
    v_sys_source             VARCHAR2(40) := 'PRC_ODM_INTRANSIT_LCFC';
    v_sys_creation_date      date := trunc(sysdate);
    v_start_time             date;
    v_sys_last_modified_date date := sysdate;

    v_count                  int;
    V_DAY                    INT := to_char(sysdate, 'D');

  BEGIN
    exitcode         := -20099;
    logger.wfl_id    := wfl_id;
    logger.node_id   := node_id;
    logger.log_level := 'ALL';

    pkg_base_funcs.init_log_run(g_logic_name);

    logger.info(l_proc_name || ' Start');


  EXECUTE IMMEDIATE 'truncate table z_ui_gr_odm_LCFC';
  EXECUTE IMMEDIATE 'truncate table z_ui_asn_odm_LCFC';

  insert into prdspoemthk.z_ui_gr_odm_LCFC(
        tpl_receipt_id,
        time_zone,
        supplier_id,
        ship_to_id,
        receipt_date,
        tpl_receipt_line_id,
        odm_pN,
        matnr,
        invoice_number,
        dest_storage_location,
        po_id ,
        po_line_id,
        odm_po,
        odm_po_line,
        gr_type,
        qty ,
        delivery_notes,
        delivery_notes_line,
        hawb,
        status,
        reason,
        site_name,
        bu ,
        ship_from_name,
        ship_to_name,
        po_qty,
        status_show)
    select  tpl_receipt_id,
      time_zone,
      supplier_id,
      ship_to_id,
      receipt_date,
      tpl_receipt_line_id,
      odm_pN,
      matnr,
      invoice_number,
      dest_storage_location,
      po_id ,
      po_line_id,
      odm_po,
      odm_po_line,
      gr_type,
      qty ,
      delivery_notes,
      delivery_notes_line,
      hawb,
      status,
      reason,
      UPPER(site_name),
      bu ,
      ship_from_name,
      ship_to_name,
      po_qty,
      status_show
      from pcgsdc.z_ui_gr_odm@prdcsen where UPPER(SITE_NAME) IN ('LCFC','COMPAL') ;
      commit;

insert into prdspoemthk.z_ui_asn_odm_LCFC(
        vbeln ,
        posnr,
        erdat,
        lfart,
        verur,
        btgew,
        ntgew,
        gewei,
        anzpk,
        lifnr,
        wadat_ist,
        matnr,
        werks,
        lgort,
        vgbel,
        vgpos,
        lfimg,
        bwart,
        category_sn,
        sernr,
        parts_type,
        unit_price,
        ship_to_id,
        invoice_number,
        odm_pn,
        odm_po,
        odm_po_line,
        delivery_notes_line,
        vmi_id,
        status,
        bu,
        ship_from_name,
        ship_to_name,
        po_qty,
        status_show,
        reason)
  select
       vbeln ,
        posnr,
        erdat,
        lfart,
        verur,
        btgew,
        ntgew,
        gewei,
        anzpk,
        lifnr,
        wadat_ist,
        matnr,
        werks,
        lgort,
        vgbel,
        vgpos,
        lfimg,
        bwart,
        category_sn,
        sernr,
        parts_type,
        unit_price,
        ship_to_id,
        invoice_number,
        odm_pn,
        odm_po,
        odm_po_line,
        delivery_notes_line,
        vmi_id,
        status,
        bu,
        ship_from_name,
        UPPER(ship_to_name),
        po_qty,
        status_show,
        reason
 from
        pcgsdc.z_ui_asn_odm@prdcsen where UPPER(ship_to_name) IN ('LCFC','COMPAL');

commit;


   /*
      INSERT INTO MID_ODM_INTRANSIT
        (VBELN,
         POSNR,
         ITEM,
         BU,
         SITE,
         QTY,
         WADAT_IST,
         TYPE,
         MEASURE,
         sys_creation_date,
         sys_last_modified_date,
         sys_source,
         SOLD2_KUNNR)

        select DELIVER_NOTE,
               '',
               A.MATERIAL,
               A.BU,
               UPPER(NVL(A.SITEID, 'X')),
               A.QTY,
               A.OCCUR_DATE,
               A.G_TYPE,
               DECODE(A.G_type,'ASN','GI','GR') as MEASURE,
                       v_sys_creation_date sys_creation_date,
                       v_sys_last_modified_date sys_last_modified_date,
                       v_sys_source sys_source,
               A.VENDOR

                  FROM rpt_detail_parts_intran A

                 WHERE a.deliver_note<>'DUMMY'
                   and exists (select 1
                          FROM prdabppoemthk.z_ui_bs_share@prdmdmn_lk_new M
                         where M.display_type <> 'FLEX'
                           AND A.MATERIAL = M.ITEM)


               ;
    */
    delete from MID_ODM_INTRANSIT where site IN ('LCFC','COMPAL');
     INSERT INTO MID_ODM_INTRANSIT
        (VBELN,
         POSNR,
         ITEM,
         BU,
         SITE,
         QTY,
         WADAT_IST,
         TYPE,
         MEASURE,
         sys_creation_date,
         sys_last_modified_date,
         sys_source,
         SOLD2_KUNNR)

        select VERUR,
               '',
               max(A.MATNR) as ITEM,
               --MAX(DECODE(A.WERKS,'H121','IdeaNB','H112','IdeaDT','H000','THINK')) BU,
               MAX(DECODE(A.WERKS,'H121','IDEANB','H112','IDEADT','H000','THINK')) BU,
               MAX(A.SHIP_TO_NAME),
               sum(A.LFIMG),
               max(to_date(A.erdat,'YYYY-MM-DD')),
               max(SHIP_FROM_NAME) as TYPE,
               'GI' as MEASURE,
               v_sys_creation_date sys_creation_date,
               v_sys_last_modified_date sys_last_modified_date,
               v_sys_source sys_source,
               max(A.SHIP_TO_NAME)

                  --FROM prdspoemthk.z_ui_asn_odm_LCFC@prdmdmn_lk_new A
                  FROM z_ui_asn_odm_LCFC A
                 WHERE a.status in('ASN_GR','ASN_SENT')
                   and exists (select 1
                          --FROM prdabppoemthk.z_ui_bs_share@prdmdmn_lk_new M
                          FROM z_ui_bs_share M
                         where M.display_type <> 'FLEX'
                           AND A.MATNR = M.ITEM)
                   AND to_date(A.erdat,'YYYY-MM-DD')< next_day(trunc(sysdate)-7,5)
                   group by VERUR


               ;
      v_count := sql%rowcount;

      logger.info(l_proc_name || ' Start GI ' || v_count);
      commit;

      --GR
           INSERT INTO MID_ODM_INTRANSIT
        (VBELN,
         POSNR,
         ITEM,
         BU,
         SITE,
         QTY,
         WADAT_IST,
         TYPE,
         MEASURE,
         sys_creation_date,
         sys_last_modified_date,
         sys_source,
         SOLD2_KUNNR)

        select Delivery_NOTES,
               '',
               M.ITEM,
               M.BU,
               M.SITE,
               -A.QTY,
               CASE
               WHEN INSTR(receipt_date , '-' , 1 , 1) = 5 THEN to_date(substr(receipt_date,1,10) , 'yyyy-mm-dd')
               WHEN receipt_date IS NOT NULL THEN to_date(replace(receipt_date,'?'),'dd-mm-yy','nls_date_language = American')
               ELSE DATE '2000-1-1'
               END receipt_date,

               M.TYPE as TYPE,
               'GR' as MEASURE,
               v_sys_creation_date sys_creation_date,
               v_sys_last_modified_date sys_last_modified_date,
               v_sys_source sys_source,
               M.SITE

                  --FROM prdspoemthk.z_ui_gr_odm_LCFC@prdmdmn_lk_new A,MID_ODM_INTRANSIT M
                  FROM z_ui_gr_odm_LCFC A,MID_ODM_INTRANSIT M

                 WHERE A.Delivery_NOTES=M.VBELN
                 AND status<>'ERROR';

     v_count := sql%rowcount;

      logger.info(l_proc_name || ' Start GR ' || v_count);
      COMMIT;

     update MID_ODM_INTRANSIT a set measure='GR_Deleted'
     where measure='GR'
     AND WADAT_IST>=next_day(trunc(sysdate)-7,5);

    commit;

        EXECUTE IMMEDIATE 'truncate table Z_ODM_BS_INTRANSIT';

       INSERT INTO Z_ODM_BS_INTRANSIT
        (ITEM,
         BU,
         SITE,
         QTY,

         MEASURE,
         sys_creation_date,
         sys_last_modified_date,
         supplierid,
         supplier_NAME,
         sys_source,
         sys_created_by)
        select ITEM,
               max(BU) BU,
               SITE,
               SUM(Qty) QTY,

               'ATS' MEASURE,
               v_sys_creation_date sys_creation_date,
               v_sys_last_modified_date sys_last_modified_date,
               MAX(supplierid),
               MAX(supplier_NAME),
               v_sys_source sys_source,
               v_sys_source sys_created_by
          FROM MID_ODM_INTRANSIT
          where site IN ('LCFC','COMPAL')
           and measure<>'GR_Deleted'
         GROUP BY ITEM, SITE , BU;

           MERGE INTO Z_ODM_BS_INTRANSIT A
      USING (SELECT item,bu,SITE, sum(qty) as qty_GI
               FROM MID_ODM_INTRANSIT
              where site IN ('LCFC','COMPAL') and measure='GI'
              group by item,bu,SITE) B
      ON (A.ITEM = B.ITEM and A.BU = B.BU AND A.SITE = B.SITE)
      WHEN MATCHED THEN
        UPDATE SET A.qty_GI = B.qty_GI;

          MERGE INTO Z_ODM_BS_INTRANSIT A
      USING (SELECT item,bu,SITE, sum(qty) as qty_GR
               FROM MID_ODM_INTRANSIT
              where site IN ('LCFC','COMPAL') and measure='GR'
              group by item,bu,SITE) B
      ON (A.ITEM = B.ITEM and A.BU = B.BU AND A.SITE = B.SITE)
      WHEN MATCHED THEN
        UPDATE SET A.qty_GR = B.qty_GR;

      logger.info(l_proc_name || ' Start supplierid');

commit;

      --supplier
      MERGE INTO Z_ODM_BS_INTRANSIT A
      USING (SELECT item,upper(bu) BU,SITEID, max(supplier) supplier,max(vendor_code) vendor_code
               --FROM prdabppoemthk.z_ui_bs_share@prdmdmn_lk_new
               FROM z_ui_bs_share
              where display_type <> 'FLEX'
              group by item,Upper(bu),SITEID) B
      ON (A.ITEM = B.ITEM and A.BU = B.BU AND A.SITE = B.SITEID)
      WHEN MATCHED THEN
        UPDATE SET A.supplierid = B.vendor_code, A.supplier_NAME = B.supplier;

     MERGE INTO Z_ODM_BS_INTRANSIT A
      USING (SELECT item,upper(bu) bu,SITEID, max(supplier) supplier,max(vendor_code) vendor_code
               --FROM prdabppoemthk.z_ui_bs_share@prdmdmn_lk_new
               FROM z_ui_bs_share
              where display_type <> 'FLEX'
              group by item,upper(bu),SITEID) B
      ON (A.ITEM = B.ITEM and A.BU = B.BU AND A.SITE = B.SITEID)
      WHEN MATCHED THEN
        UPDATE SET A.supplierid = B.vendor_code, A.supplier_NAME = B.supplier;


             MERGE INTO(SELECT * FROM  Z_ODM_BS_INTRANSIT WHERE  supplierid = 'X') A
      USING (SELECT item,'THINK' bu,SITEID, max(supplier) supplier,max(vendor_code) vendor_code
               --FROM prdabppoemthk.z_ui_bs_share@prdmdmn_lk_new
               FROM z_ui_bs_share
              where display_type <> 'FLEX'  AND BU in('ThinkNB','ThinkDT')
              group by item,SITEID) B
      ON (A.ITEM = B.ITEM  AND A.SITE = B.SITEID and a.bu=b.bu)
      WHEN MATCHED THEN
        UPDATE SET A.supplierid = B.vendor_code, A.supplier_NAME = B.supplier;

           MERGE INTO(SELECT * FROM  Z_ODM_BS_INTRANSIT WHERE  supplierid = 'X') A
      USING (SELECT item,SITEID, max(supplier) supplier,max(vendor_code) vendor_code
               --FROM prdabppoemthk.z_ui_bs_share@prdmdmn_lk_new
               FROM z_ui_bs_share
              where display_type <> 'FLEX'  AND BU='ALL'
              group by item,SITEID) B
      ON (A.ITEM = B.ITEM  AND A.SITE = B.SITEID)
      WHEN MATCHED THEN
        UPDATE SET A.supplierid = B.vendor_code, A.supplier_NAME = B.supplier;


            MERGE INTO(SELECT * FROM  Z_ODM_BS_INTRANSIT WHERE  supplierid = 'X') A
      USING (SELECT item,upper(bu) bu,SITEID, max(supplier) supplier,max(vendor_code) vendor_code
               --FROM prdabppoemthk.z_ui_bs_share@prdmdmn_lk_new
               FROM z_ui_bs_share
              where display_type <> 'FLEX'  AND SITEID = 'ODM'
              group by item,upper(bu),SITEID) B
      ON (A.ITEM = B.ITEM  AND A.BU = B.BU )
      WHEN MATCHED THEN
        UPDATE SET A.supplierid = B.vendor_code, A.supplier_NAME = B.supplier;


            MERGE INTO(SELECT * FROM  Z_ODM_BS_INTRANSIT WHERE  supplierid = 'X') A
      USING (SELECT item,'THINK' bu,SITEID, max(supplier) supplier,max(vendor_code) vendor_code
               --FROM prdabppoemthk.z_ui_bs_share@prdmdmn_lk_new
               FROM z_ui_bs_share
              where display_type <> 'FLEX'  AND SITEID = 'ODM' AND BU in('ThinkNB','ThinkDT')
              group by item,SITEID) B
      ON (A.ITEM = B.ITEM  AND A.BU = B.BU )
      WHEN MATCHED THEN
        UPDATE SET A.supplierid = B.vendor_code, A.supplier_NAME = B.supplier;

            MERGE INTO(SELECT * FROM  Z_ODM_BS_INTRANSIT WHERE  supplierid = 'X') A
      USING (SELECT item, upper(bu) bu,SITEID, max(supplier) supplier,max(vendor_code) vendor_code
               --FROM prdabppoemthk.z_ui_bs_share@prdmdmn_lk_new
               FROM z_ui_bs_share
              where display_type <> 'FLEX'  AND SITEID = 'ODM' AND  BU = 'ALL'
              group by item,upper(bu),SITEID) B
      ON (A.ITEM = B.ITEM  )
      WHEN MATCHED THEN
        UPDATE SET A.supplierid = B.vendor_code, A.supplier_NAME = B.supplier;



      commit;


      logger.info(l_proc_name || ' Start EXP_PART_INVENTORY_SCC');



      delete from PRDPCDW.EXP_PART_INVENTORY_SCC
       where sys_created_by = v_sys_source;
commit;

      /*insert into PRDPCDW.EXP_PART_INVENTORY_SCC
        (siteid,
         item,
         supplier,
         sys_created_by,
         sys_creation_date,
         bu,
         intransit,
         FAMILY,
         FLAG,
         soi,
         loi)
        select SITE,
               item,
               \*supplier_NAME*\SUPPLIERID,
               v_sys_source sys_created_by,
               v_sys_last_modified_date sys_creation_date,
               DECODE(bu,'THINK','THINKNB',BU) BU,
               sum(qty),
               'X' FAMILY,
               'ODM' FLAG,
               0 soi,
               0 loi
          from Z_ODM_BS_INTRANSIT
          where bu is not null
          and site IN ('LCFC','COMPAL')
         group by SITE, item, \*supplier_NAME*\SUPPLIERID, bu
         having sum(qty)<>0;--add wanggang38 2022-7-27

      COMMIT;*/--edit by xuml7

      DELETE FROM LCFC_INTRANSIT;
      COMMIT;
      INSERT INTO LCFC_INTRANSIT
        SELECT *
          FROM PRDPCDW.EXP_PART_INVENTORY_SCC
         WHERE SYS_CREATED_BY = 'PRC_ODM_INTRANSIT_LCFC';
      COMMIT;

    logger.info(l_proc_name || ' Start z_odm_intransit_bak');
    delete from z_odm_intransit_bak where to_date(version_name,'yyyy-mm-dd')<trunc(sysdate-180);
      insert into z_odm_intransit_bak
        (siteid,
         item,
         supplier,
         sys_created_by,
         sys_creation_date,
         bu,
         intransit,
         version_name)
        select site,
               item,
               supplierID,
               sys_created_by,
               v_sys_last_modified_date sys_creation_date,
               bu,
               QTY,
               to_char(to_date(trunc(sysdate)),'yyyymmdd')
          from Z_ODM_BS_INTRANSIT
         where sys_created_by = v_sys_source;



      commit;




    logger.info(l_proc_name || ' Completed');
    exitcode := 0;

      exception
    when others then
      exitcode := sqlcode;
      logger.error;
      raise;
 END;
 END;
