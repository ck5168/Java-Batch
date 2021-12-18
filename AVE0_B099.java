package com.ck.av.e0.batch;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.map.MultiKeyMap;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.ck.av.module.AV_BatchBean;
import com.ck.common.exception.ModuleException;
import com.ck.common.hr.DivData;
import com.ck.common.hr.PersonnelData;
import com.ck.common.hr.WorkDate;
import com.ck.common.util.DATE;
import com.ck.common.util.FieldOptionList;
import com.ck.common.util.LoginUtil;
import com.ck.common.util.batch.CountManager;
import com.ck.common.util.batch.ErrorLog;
import com.ck.common.util.mail.MailSender;
import com.igsapp.db.BatchQueryDataSet;

/**
 * 	Date    Version Description Author

 */
@SuppressWarnings("unchecked")
public class AVE0_B099 extends AV_BatchBean { 
    
    private static final Logger log = Logger.getLogger(AVE0_B099.class);

    private static final String JOB_NAME = "JADJDC903"; 

    private static final String PROGRAM = "DJC9_B003"; 

    private static final String BUSINESS = "DJ"; 

    private static final String SUBSYSTEM = "C9"; 

    private static final String PERIOD = "Day";    

    
    private static final boolean isAUTO_WRITELOG = false;

   
    private static final String QUERY_COUNT = "QueryCount";

   
    private static final String QUERY_MAIL_COUNT = "ReceiveCount";

    
    private static final String ERROR_COUNT = "ErrorCount";

    private static MultiKeyMap toMap;
    
    private BatchQueryDataSet bqds;
    
    private ErrorLog errorLog;  
    
    private CountManager countManager;

    private StringBuilder sb;

    
    private final String emailTitle;

    private static final String SQL_QUERY_001 = "com.ck.av.e0.batch.AVE0_B099.SQL_QUERY_001";

    
    public AVE0_B099() throws Exception {        
        
        super(JOB_NAME, PROGRAM, BUSINESS, SUBSYSTEM, PERIOD, log, isAUTO_WRITELOG);
        
        countManager = new CountManager(JOB_NAME, PROGRAM, BUSINESS, SUBSYSTEM, PERIOD);

        errorLog = new ErrorLog(JOB_NAME, PROGRAM, BUSINESS, SUBSYSTEM);

        this.initCountManager();

        sb = new StringBuilder();

        sb.append("<META http-equiv=Content-Type content='text/html; charset=BIG5'></META>\n");
        sb.append("<html>\n");
        sb.append("<head></head>\n" + "<body bgColor=#FFFFFF>\n");

        sb.append("<div>");
        sb.append("&nbsp");
        sb.append("</div>");

        sb.append("<table border=1>\n<tr>");
        sb.append("<th ><font><b>FILENO</b></font></th>");
        sb.append("<th ><font><b>TRANTYPE</b></font></th>");
        sb.append("<th ><font><b>ACPTDATE</b></font></th>");
        sb.append("<th ><font><b>ACPTNO</b></font></th>");
        sb.append("<th ><font><b>CUSTOMID</b></font></th>");
        sb.append("<th ><font><b>ERROR</b></font></th></tr>\n");

        emailTitle = sb.toString();

        sb.setLength(0);

    }

    
    public void execute(String[] args) throws Exception {

        try {

            toMap = new MultiKeyMap();
                        
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            String PRE_WORK_DATE = sdf.format(WorkDate.getXWorkingDate(java.sql.Date.valueOf(DATE.getDBDate()), 1, false));
            
            if(args.length == 1 && DATE.isDate(args[0])) {
                PRE_WORK_DATE = args[0];
            }            
            
            String INPUT_DATE_S = PRE_WORK_DATE + " 19:00:00.000000";
            String INPUT_DATE_E = DATE.getDBTimeStamp();
            log.fatal("INPUT_DATE_S===" + INPUT_DATE_S);
            log.fatal("INPUT_DATE_E===" + INPUT_DATE_E);

            bqds = getBatchQueryDataSet();

            executeBySendMail(SQL_QUERY_001, INPUT_DATE_S, INPUT_DATE_E);

        } catch (Exception e) {
            log.error("", e);
            setExitCode(ERROR);
        } finally {

            log.error(countManager);

            if (countManager != null) {
                countManager.writeLog();
            }

            if (bqds != null) {
                bqds.close();
            }

            printExitCode(getExitCode());

        }

    }

    private void executeBySendMail(String SQL_QUERY_001, String INPUT_DATE_S, String INPUT_DATE_E) throws Exception {

        try {
            log.fatal("==================== QUERY_STR_TIME = " + DATE.getDBTimeStamp());

            bqds.clear();

            bqds.setField("INPUT_DATE_S", INPUT_DATE_S);
            bqds.setField("INPUT_DATE_E", INPUT_DATE_E);
            searchAndRetrieve(bqds, SQL_QUERY_001);

            log.fatal("==================== QUERY_END_TIME = " + DATE.getDBTimeStamp());

        } catch (Exception e) {
            errorLog.addErrorLog("QUERY_ERROR", e);
            int errorCount = errorLog.getErrorCountAndWriteErrorMessage();
            countManager.addCountNumber(ERROR_COUNT, errorCount);
            throw e;
        }

        int inputCount = getInputCount();

        countManager.addCountNumber(QUERY_COUNT, inputCount); 

        if (inputCount == 0) {
            log.fatal("NODATA");
            return;
        }

        try {            
            
            List<String> RCV_LIST = new ArrayList();
            
            RCV_LIST.addAll(FieldOptionList.getName("AV", "AVE0_B099_RCV").keySet());
           
            DivData dd = new DivData();
            PersonnelData pd = new PersonnelData();
            int mailcount = 0;
            int queryMailCount = 0;
            String RCV_EMAIL = "";
            for (String RCV_ID : RCV_LIST) {

                
                queryMailCount++;

                if(!LoginUtil.isProd()) {

                    if (RCV_ID.length() == 7) {
                        try {
                            log.fatal("\n\n RCV_ID-->"+RCV_ID);
                            RCV_EMAIL = dd.getUnit(RCV_ID).getDivEmail();
                            log.fatal("\n\n RCV_EMAIL-->"+RCV_EMAIL);
                            if(StringUtils.isNotBlank(RCV_EMAIL)){
                                toMap.put("admin@cklife.com.tw", MailSender.DEFAULT_MAIL, "admin@cklife.com.tw");
                                mailcount++;
                            }else{
                                log.fatal("ERROR_DIV:"+RCV_ID);
                            }
                        }catch(Exception ex) {
                            log.fatal("ERROR_DIV:"+RCV_ID);
                            log.fatal("NO_DIV email",ex);
                        }
                    } else if (RCV_ID.length() == 10) {
                        try {
                            log.fatal("\n\n RCV_ID-->"+RCV_ID);
                            RCV_EMAIL = pd.getByEmployeeID2(RCV_ID).getEmail();
                            log.fatal("\n\n RCV_EMAIL-->"+RCV_EMAIL);
                            if(StringUtils.isNotBlank(RCV_EMAIL)){
                                toMap.put("iris_yeh@cklife.com.tw", MailSender.DEFAULT_MAIL, "iris_yeh@cklife.com.tw");
                                mailcount++;
                            }else{
                                log.fatal("ERROR_ID:"+RCV_ID);
                            }
                        }catch(Exception ex) {
                            log.fatal("ERROR_ID:"+RCV_ID);
                            log.fatal("NO ID email",ex);
                        }
                    }
                } else {
                    if (RCV_ID.length() == 7) {
                        try {
                            RCV_EMAIL = dd.getUnit(RCV_ID).getDivEmail();
                            if(StringUtils.isNotBlank(RCV_EMAIL)){
                                toMap.put(RCV_EMAIL, MailSender.DEFAULT_MAIL, RCV_EMAIL);
                                mailcount++;
                            }else{
                                log.fatal("ERROR_DIV:"+RCV_ID);
                            }
                        }catch(Exception ex) {
                            log.fatal("ERROR_DIV:"+RCV_ID);
                            log.fatal("NO_DIVemail",ex);
                        }
                    } else if (RCV_ID.length() == 10) {
                        try {
                            RCV_EMAIL = pd.getByEmployeeID2(RCV_ID).getEmail();
                            if(StringUtils.isNotBlank(RCV_EMAIL)){
                                toMap.put(RCV_EMAIL, MailSender.DEFAULT_MAIL, RCV_EMAIL);
                                mailcount++;
                            }else{
                                log.fatal("ERROR_ID:"+RCV_ID);
                            }
                        }catch(Exception ex) {
                            log.fatal("ERROR_ID:"+RCV_ID);
                            log.fatal("no ID email",ex);
                        }
                    }
                }
            }

            countManager.addCountNumber(QUERY_MAIL_COUNT, mailcount);

            if (!toMap.isEmpty()) {
                sendMail();                
            }

        } catch (Exception e) {
            setExitCode(ERROR); 
            log.fatal("ERROR", e);
            throw e; 
        } finally {                      
            int errorCount = errorLog.getErrorCountAndWriteErrorMessage();            
            countManager.addCountNumber(ERROR_COUNT, errorCount);
        }
    }

    /**
     * sendMail
     * @param dataList
     * @throws Exception
     */
    private void sendMail() throws Exception {

        try {

            sb.append(emailTitle);

            for (prepareFetch(); fetchData(bqds); goNext()) {

                while (bqds.next()) {
                    String IMG_KIND = ObjectUtils.toString(bqds.getField("IMG_KIND"));
                    String TRN_KIND = ObjectUtils.toString(bqds.getField("TRN_KIND"));
                    String APLY_DATE = ObjectUtils.toString(bqds.getField("APLY_DATE"));
                    String RCPT_NO = ObjectUtils.toString(bqds.getField("RCPT_NO"));
                    String CUST_ID = ObjectUtils.toString(bqds.getField("CUST_ID"));
                    String ERROR_LOG = ObjectUtils.toString(bqds.getField("ERROR_LOG"));                    

                    sb.append("<tr>");
                    sb.append("<td><font><center>").append(IMG_KIND).append("</center></font></td>");
                    sb.append("<td><font><center>").append(TRN_KIND).append("</center></font></td>");
                    sb.append("<td><font><center>").append(APLY_DATE).append("</center></font></td>");
                    sb.append("<td><font><center>").append(RCPT_NO).append("</center></font></td>");
                    sb.append("<td><font><center>").append(CUST_ID).append("</center></font></td>");
                    sb.append("<td><font><center>").append(ERROR_LOG).append("</center></font></td>");
                    sb.append("</tr>");

                }
            }
            sb.append("</table>");

            String content = sb.toString();
            
            for(Object key : toMap.keySet()) {
                log.fatal("MAIL TO = " + key);
            }

            MailSender.sendHtmlMail(toMap, "IMAGE_ERROR", content);

        } finally {
            sb.setLength(0);
        }

    }

    /**
     * initCountManager
     */
    private void initCountManager() throws ModuleException {
        countManager.createCountType("START");
        countManager.writeLog();
        countManager.clearCountTypeAndNumber();
        countManager.createCountType(QUERY_COUNT);
        countManager.createCountType(QUERY_MAIL_COUNT);
        countManager.createCountType(ERROR_COUNT);
    }
}
