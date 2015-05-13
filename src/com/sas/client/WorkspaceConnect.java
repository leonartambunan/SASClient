package com.sas.client;

import com.sas.iom.SAS.ILanguageService;
import com.sas.iom.SAS.ILanguageServicePackage.CarriageControlSeqHolder;
import com.sas.iom.SAS.ILanguageServicePackage.LineTypeSeqHolder;
import com.sas.iom.SAS.IWorkspace;
import com.sas.iom.SASIOMDefs.StringSeqHolder;
import com.sas.iom.WorkspaceConnector;
import com.sas.iom.WorkspaceFactory;

import java.util.Properties;

public class WorkspaceConnect {

    public static void main(String[] args) throws Exception {

        String prd1 = "1";

        Properties iomServerProperties = new Properties();

        iomServerProperties.put("host", "CLO-UAT-SAS");

        iomServerProperties.put("port", "8591");

        iomServerProperties.put("userName", "sasdemo");

        iomServerProperties.put("password", "Student1");

        Properties[] serverList = {iomServerProperties};

        WorkspaceFactory wFactory = new WorkspaceFactory(serverList,null,null);

        WorkspaceConnector connector = wFactory.getWorkspaceConnector(0L);

        String wName = connector.getWorkspace().Name();

        System.out.println(wName);

        IWorkspace workspace = connector.getWorkspace();

        ILanguageService sasLanguage = workspace.LanguageService();
//        sasLanguage.Submit("data sasuser.testset;x=1;run;");

//        sasLanguage.Submit("data a;x=1;run;proc print;run;");


        long startTime = System.currentTimeMillis();

        sasLanguage.Submit("" +
                "LIBNAME ERP_SRC ODBC DATASRC=ERP_SOURCE SCHEMA=dbo  USER='scdtpol_sas'  PASSWORD='P@ssw0rd';\n" +
                "PROC SQL;           \n" +
                "CREATE TABLE ERPWORK.BUNGA_PRK AS              \n" +
                "     SELECT DISTINCT\n" +
                "                CASE WHEN E.COD_LOB IS NULL THEN 32\n" +
                "                ELSE E.COD_LOB\n" +
                "                END AS LOB,  A.COD_ACCT_NO, A.COD_ARREAR_TYPE, SUM(A.AMT_ARREARS_DUE) AS AMT_ARREARS_DUE\n" +
                "     FROM ERP_SRC.BD_MIS_CH_ARREARS_TABLE A         \n" +
                "     LEFT JOIN (SELECT * FROM ERP_SRC.BD_BA_CUST_ACCT_AO_LOB_XREF WHERE AS_OF_DATE = '2015-02-28') E ON  A.COD_ACCT_NO = E.COD_ACCT_NO AND E.TYP_ENTITY = 'A'          \n" +
                "     WHERE A.COD_ARREAR_TYPE IN ('I','N') AND A.AMT_ARREARS_DUE <> 0 AND A.AS_OF_DATE = '2015-02-28'    \n" +
                "     GROUP BY LOB,  A.COD_ACCT_NO, A.COD_ARREAR_TYPE;          \n" +
                "QUIT;         ") ;

//                "proc print data=QUERY_FOR_APPLICATION_STRESS(obs=50); " +
//                "run;"
//        );

        long endTime = System.currentTimeMillis();

        System.out.println("Time consumption: "+(endTime-startTime));

        CarriageControlSeqHolder logCarriageControlHldr = new CarriageControlSeqHolder();

        LineTypeSeqHolder logLineTypeHldr = new LineTypeSeqHolder();

        StringSeqHolder logHldr = new StringSeqHolder();

        sasLanguage.FlushLogLines(Integer.MAX_VALUE,logCarriageControlHldr, logLineTypeHldr,logHldr);

        String[] logLines = logHldr.value;

        CarriageControlSeqHolder listCarriageControlHldr = new CarriageControlSeqHolder();

        LineTypeSeqHolder listLineTypeHldr = new LineTypeSeqHolder();

        StringSeqHolder listHldr = new StringSeqHolder();

        sasLanguage.FlushListLines(Integer.MAX_VALUE,listCarriageControlHldr,listLineTypeHldr,listHldr);

        String[] listLines = listHldr.value;

        long end2Time = System.currentTimeMillis();

        System.out.println("Total Time = "+(end2Time-startTime));

        for (String listLine : listLines) {
            System.out.println(listLine);
        }

        for (String logLine: logLines) {
            System.out.println(logLine);
        }

        wFactory.shutdown();
        connector.close();
//
//        WorkspaceFactory wFactory = new WorkspaceFactory(serverList,null,null);
//        WorkspaceConnector connector = wFactory.getWorkspaceConnector(0L);
////        IWorkspace sasWorkspace = connector.getWorkspace();
////        ILanguageService sasLanguage = sasWorkspace.LanguageService();
//
////        IWorkspace workspace = connector.getWorkspace();
//
//        IWorkspace climWorkspace = wFactory.createWorkspaceByServer(iomServerProperties);
//
//        ILanguageService climLang = climWorkspace.LanguageService();
//
//        //Acquire the Stored process service
//
//        IStoredProcessService climSP = climLang.StoredProcessService();
//
//        //Set the repository of the Stored process
//
//        climSP.Repository("file:/SAS Folders/PSAK/PSAK_AML");
//
//        //Set the program and supply the required Parameters
//
//        climSP.Execute("sample1", " prd1=" + prd1);
////prd1 - can be the user supplied Parameters!
//
//        //JDBC Connect using MVA
//
//        IDataService climDataService = climWorkspace.DataService();
//
//        java.sql.Connection climconnect = new MVAConnection(climDataService, new Properties());
//
//        java.sql.Statement statement = climconnect.createStatement();
//
//        java.sql.ResultSet rs = statement.executeQuery("Select * from work.one");
//
//        while (rs.next()) {
//
//            String region = rs.getString("Region");
//        }
    }
}