package com.ck.av.e0.batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.keyvalue.MultiKey;
import org.apache.commons.collections.map.MultiKeyMap;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.ck.aa.x0.module.AA_X0Z020;
import com.ck.aa.x0.module.AA_X0Z024;
import com.ck.av.module.AV_BatchBean;
import com.ck.common.exception.ModuleException;
import com.ck.common.hr.DivData;
import com.ck.common.hr.PersonnelData;
import com.ck.common.util.DATE;
import com.ck.common.util.FieldOptionList;
import com.ck.common.util.STRING;
import com.ck.common.util.batch.CountManager;
import com.ck.common.util.batch.ErrorLog;
import com.ck.common.util.mail.MailSender;
import com.ck.fm.z0.bo.DTFMZ003_bo;


@SuppressWarnings({ "unchecked", "rawtypes" })
public class AVE0_B933 extends AV_BatchBean {

    private static final Logger log = Logger.getLogger(AVE0_B933.class);
   
    private static final String JOB_NAME = "JAAVE033";
    
    private static final String PROGRAM = "AVE0_B933";
    
    private static final String BUSINESS = "AV";
    
    private static final String SUBSYSTEM = "E0";
    
    private static final String PERIOD = "¤é";
    
    private static final boolean isAUTO_WRITELOG = false;
    
    private ErrorLog errorLog;
   
    private CountManager countManager;
    
    private static final String INPUT_COUNT = "INPUT_CNT";
    
    private static final String ERROR_COUNT = "ERROR_CNT";

    private StringBuilder sb;

    private StringBuilder sb1;

    private Map MODEXP_MAP = new HashMap();
    
    public AVE0_B933() throws Exception {

        super(JOB_NAME, PROGRAM, BUSINESS, SUBSYSTEM, PERIOD, log, isAUTO_WRITELOG);

        countManager = new CountManager(JOB_NAME, PROGRAM, BUSINESS, SUBSYSTEM, PERIOD);

        errorLog = new ErrorLog(JOB_NAME, PROGRAM, BUSINESS, SUBSYSTEM);

        initCountManager();

        sb = new StringBuilder();
        sb1 = new StringBuilder();
    }

    public void execute(String[] args) throws Exception {

        try {

            String currentDate = DATE.getDBDate();

            Map<String, String> IPT_MAP = new HashMap<String, String>();
            if (args == null || ArrayUtils.isEmpty(args)) {

                IPT_MAP.put("TYPE", "1");
                
                IPT_MAP.put("BGN_DATE", currentDate);
                IPT_MAP.put("END_DATE", currentDate);
                             

                log.fatal("no args, DATE¡G" + currentDate);

            } else {

                this.getInputParams(args, IPT_MAP, currentDate);
                if (getExitCode() != OK) {
                    return;
                }

            }

            String TYPE = MapUtils.getString(IPT_MAP, "TYPE");
            String BGN_DATE = MapUtils.getString(IPT_MAP, "BGN_DATE");
            String END_DATE = MapUtils.getString(IPT_MAP, "END_DATE");            
            String OLD_DIV_NO = "";
            String NEW_DIV_NO = "";
            String NEW_DIV_NM = "";
            
            List<Map> CHG_LIST = new ArrayList<Map>();
            if ("1".equals(TYPE)) {
                List<DTFMZ003_bo> rtnList;
                try {
                    rtnList = new DivData().getUnitTransfer(BGN_DATE, END_DATE, "B");
                    log.fatal("CNT:" + rtnList.size());
                } catch (Exception e) {
                    setExitCode(ERROR);
                    log.fatal("CNT_ERROR", e);
                    errorLog.addErrorLog("ERR", "Moudle Error");
                    errorLog.getErrorCountAndWriteErrorMessage();
                    return;
                }

                for (DTFMZ003_bo bo : rtnList) {
                    String TRUTH_DATE = bo.getTRUTH_DATE();
                    if (StringUtils.isBlank(TRUTH_DATE)) {
                        setExitCode(ERROR);
                        log.fatal("DATE is empty");
                        return;
                    }
                    Map<String, String> CHG_MAP = new HashMap();
                    CHG_MAP.put("OLD_DIV_NO", bo.getDIV_NO());
                    CHG_MAP.put("NEW_DIV_NO", bo.getNEW_DIV_NO());
                    CHG_MAP.put("EFT_DATE", TRUTH_DATE);
                    CHG_MAP.put("NEW_DIV_NM", bo.getNewUnit().getDivShortName());
                    CHG_LIST.add(CHG_MAP);

                    log.fatal("LIST CHG_MAP¡G" + CHG_MAP);
                }
            } else {
                OLD_DIV_NO = MapUtils.getString(IPT_MAP, "OLD_DIV");
                NEW_DIV_NO = MapUtils.getString(IPT_MAP, "NEW_DIV");
                NEW_DIV_NM = MapUtils.getString(IPT_MAP, "NEW_NM");

                Map<String, String> CHG_MAP = new HashMap();
                CHG_MAP.put("OLD_DIV_NO", OLD_DIV_NO);
                CHG_MAP.put("NEW_DIV_NO", NEW_DIV_NO);
                CHG_MAP.put("EFT_DATE", BGN_DATE);
                CHG_MAP.put("NEW_DIV_NM", NEW_DIV_NM);
                CHG_LIST.add(CHG_MAP);

                log.fatal("LIST CHG_MAP¡G" + CHG_MAP);
            }

            sb.append("args\n TYPE¡G").append(TYPE).append("\n BGN_DATE¡G").append(BGN_DATE).append("\n END_DATE¡G").append(END_DATE)
                    .append("\n EXE_MOD_ARY¡G");

            sb.append("\n OLD_DIV_NO¡G").append(OLD_DIV_NO).append("\n NEW_DIV_NO¡G").append(NEW_DIV_NO).append("\n NEW_DIV_NM¡G")
                    .append(NEW_DIV_NM);
            log.fatal(sb.toString());
            sb.setLength(0);

            
            countManager.addCountNumber(INPUT_COUNT, CHG_LIST.size());
           
            sb1.append("Execute¡G");
            try {                
                try {
                    sb1.append("AA_X0Z024¡A");
                    appendMailContent(new AA_X0Z024().batchChangeDIVNO_DTAAX024(CHG_LIST));
                } catch (Exception e) {
                    MODEXP_MAP.put("CALL batchChangeDIVNO_DTAAX024 Error ", "CALL batchChangeDIVNO_DTAAX024 Error ");
                    errorHandle(e);
                }
                try {
                    sb1.append("AA_X0Z020¡A");
                    appendMailContent(new AA_X0Z020().batchChangeDIVNO_DTAAX020(CHG_LIST));
                } catch (Exception e) {
                    MODEXP_MAP.put("CALL batchChangeDIVNO_DTAAX020 Error ", "CALL batchChangeDIVNO_DTAAX020 Error ");
                    errorHandle(e);
                }
               

            } finally {
                int count = errorLog.getErrorCountAndWriteErrorMessage();
                countManager.addCountNumber(ERROR_COUNT, count);
                log.fatal(sb1.toString());
                sb1.setLength(0);
            }
            
            try {

                if (sb.length() > 0 || !(MODEXP_MAP == null || MODEXP_MAP.isEmpty())) {
                    List<String> RCV_LIST = new ArrayList();
                    try {
                        RCV_LIST.addAll(FieldOptionList.getName("AV", "AVE0_B933_RCV").keySet());

                    } catch (Exception e) {
                        setExitCode(ERROR);
                        log.fatal("EMAIL Error", e);
                        errorLog.addErrorLog("EMAIL Error", "Get EMAIL Error");
                        errorLog.getErrorCountAndWriteErrorMessage();
                        return;
                    }

                    MultiKeyMap tmpMap = new MultiKeyMap();
                    log.fatal("****** RCV_LIST: " + RCV_LIST);
                    PersonnelData pd = new PersonnelData();
                    String RCV_EMAIL = ""; 
                    for (String RCV_ID : RCV_LIST) {
                        RCV_EMAIL = pd.getByEmployeeID2(RCV_ID).getEmail();
                        tmpMap.put(RCV_EMAIL, MailSender.DEFAULT_MAIL, RCV_EMAIL);

                    }
                    
                    if (sb.length() > 0) {
                        log.fatal("EMAIL Notice");
                        sb.insert(0, "<table border='1' cellpadding='0' cellspacing='1' width='50%'>");
                        sb.append("</table>");
                    } else {
                        log.fatal("CALL Module Error, send EMAIL");
                    }
                    sb.insert(0, appendMailContent(CHG_LIST));

                    if (!(MODEXP_MAP == null || MODEXP_MAP.isEmpty())) {
                        sb.append("<BR><BR> AVE0_B933 CALL module error: <BR>");
                        Iterator entries = MODEXP_MAP.entrySet().iterator();
                        while (entries.hasNext()) {
                            Entry thisEntry = (Entry) entries.next();
                            sb.append(thisEntry.getValue()).append("<BR>");
                        }
                    }

                    sb.append("</body></html>");

                    MailSender.sendHtmlMail(tmpMap, "Notice", sb.toString());
                } else {
                    log.fatal("No Update");
                }

            } catch (Exception e) {
                setExitCode(ERROR);
                log.fatal("EMAIL Error", e);
                log.fatal("", e);
                errorLog.addErrorLog("Send EMAIL Error", e);
                errorLog.getErrorCountAndWriteErrorMessage();
                return;
            }

        } catch (Exception e) {
            setExitCode(ERROR);
            log.fatal("Execute Error", e);
        } finally {
            log.fatal(countManager);

            if (countManager != null) {
                countManager.writeLog();
            }
        }

    }

    /**
     * initCountManager
     * @throws ModuleException
     */
    private void initCountManager() throws ModuleException {
        countManager.createCountType("START");
        countManager.writeLog();
        countManager.clearCountTypeAndNumber();
        countManager.createCountType(INPUT_COUNT);
        countManager.createCountType(ERROR_COUNT);
    }

    /**
     * getInputParams
     * @param args
     * @param IPT_MAP
     * @param currentDate
     */
    private void getInputParams(String[] args, Map IPT_MAP, String currentDate) {

        int length = args.length;
        if (length % 2 != 0) {
            setExitCode(ERROR);
            log.fatal("Input Error");
            return;
        }

        for (int i = 0; i < args.length; i += 2) {
            String input1 = args[i];
            String input2 = args[i + 1];

            if (input1.indexOf("$") != 0 || StringUtils.isBlank(input2)) {
                setExitCode(ERROR);
                log.fatal("Input Error¡Astar with $");
                return;
            }

            IPT_MAP.put(input1.replace("$", ""), input2);
        }

        String TYPE = MapUtils.getString(IPT_MAP, "TYPE");
        if (StringUtils.isNotBlank(TYPE)) {
            if (!"1".equals(TYPE) && !"2".equals(TYPE)) {
                setExitCode(ERROR);
                log.fatal("Input Error¡ATYPE only 1or 2¡C TYPE = " + TYPE);
                return;
            } else if ("2".equals(TYPE)) {
                String[] keys = new String[] { "OLD_DIV", "NEW_DIV", "NEW_NM", "BGN_DATE" };
                for (String key : keys) {
                    if (StringUtils.isBlank(MapUtils.getString(IPT_MAP, key))) {
                        setExitCode(ERROR);
                        log.fatal(sb.append("Input Error¡A").append(key).append("Not null¡C").toString());
                        sb.setLength(0);
                        return;
                    }
                }
            }
        } else {
            IPT_MAP.put("TYPE", "1");
        }

        String BGN_DATE = MapUtils.getString(IPT_MAP, "BGN_DATE");
        boolean isBGN_DATEblank = StringUtils.isBlank(BGN_DATE);
        if (!isBGN_DATEblank) {
            if (!DATE.isDate(BGN_DATE)) {
                setExitCode(ERROR);
                log.fatal("Input Error¡ABGN_DATE format error¡CBGN_DATE = " + BGN_DATE);
                return;
            }
        } else {
            IPT_MAP.put("BGN_DATE", currentDate);
            BGN_DATE = currentDate;
        }

        String END_DATE = MapUtils.getString(IPT_MAP, "END_DATE");
        if (StringUtils.isNotBlank(END_DATE)) {
            if (!DATE.isDate(END_DATE)) {
                setExitCode(ERROR);
                log.fatal("Input Error¡AEND_DATE format error¡CEND_DATE = " + END_DATE);
                return;
            }
            if (isBGN_DATEblank || DATE.diffDay(BGN_DATE, END_DATE) < 0) {
                setExitCode(ERROR);
                log.fatal("Input Error¡AEND_DATE, BGN_DATE not empty¡AEND_DATE>=BGN_DATE¡C");
                return;
            }
        } else {
            IPT_MAP.put("END_DATE", BGN_DATE);
        }

        String EXE_MOD = MapUtils.getString(IPT_MAP, "EXE_MOD");
        if (StringUtils.isBlank(EXE_MOD)) {
            IPT_MAP.put("EXE_MOD", "ALL");
        }

    }

    /**
     * appendMailContent
     */
    private void appendMailContent(Map tmpMap) {

        if (tmpMap == null || tmpMap.isEmpty()) {
            return;
        }

        Iterator<String> it = tmpMap.keySet().iterator();
        while (it.hasNext()) {
            String key = it.next();

            MultiKeyMap mkMap = (MultiKeyMap) tmpMap.get(key);
            if (mkMap.isEmpty()) {
                continue;
            }

            sb.append("<tr>");
            sb.append("<td>").append(key).append("</td>");
            sb.append("<td>");
            Iterator<MultiKey> it_ = mkMap.keySet().iterator();
            while (it_.hasNext()) {
                MultiKey mk = it_.next();
                Object[] objs = mk.getKeys();
                sb.append("Old Div¡G").append(STRING.objToStr(objs[1], "")).append("¡@New Div¡G").append(STRING.objToStr(objs[0], "")).append("¡@Update¡G")
                        .append(STRING.objToStr(mkMap.get(mk), "")).append("CNT");
                if (it_.hasNext()) {
                    sb.append("<BR>");
                }
            }
            sb.append("</td>");
            sb.append("</tr>");
        }

    }

    /**
     * appendMailContent
     */
    private String appendMailContent(List<Map> CHG_LIST) {

        StringBuilder sbf = new StringBuilder();
        sbf.append("<META http-equiv=Content-Type content='text/html; charset=BIG5'></META>");
        sbf.append("<html>");
        sbf.append("<head></head>");
        sbf.append("<body>");
        sbf.append("<div>").append("Update List¡G");
        for (Map CHG_MAP : CHG_LIST) {
            sbf.append("<BR>");
            sbf.append("¡@¡@¡@").append("Old div¡G").append(MapUtils.getString(CHG_MAP, "OLD_DIV_NO", ""));
            sbf.append("¡@").append("New div¡G").append(MapUtils.getString(CHG_MAP, "NEW_DIV_NO", "")).append("¡@")
                    .append(MapUtils.getString(CHG_MAP, "NEW_DIV_NM", ""));
            sbf.append("¡@").append("Date¡G").append(MapUtils.getString(CHG_MAP, "EFT_DATE", ""));
        }
        sbf.append("</div>");
        sbf.append("<BR><BR>");
        sbf.append("<div>").append("Confirm").append("</div>");
        sbf.append("<BR><BR>");

        return sbf.toString();

    }

    private void errorHandle(Exception e) {
        setExitCode(ERROR);
        log.fatal("CALL Moudle error", e);
        errorLog.addErrorLog("CALL Moudle error", e);
    }
}
