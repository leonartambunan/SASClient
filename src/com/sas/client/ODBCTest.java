package com.sas.client;

import com.sas.iom.SAS.ILanguageService;
import com.sas.iom.SAS.ILanguageServicePackage.CarriageControlSeqHolder;
import com.sas.iom.SAS.ILanguageServicePackage.LineTypeSeqHolder;
import com.sas.iom.SAS.IWorkspace;
import com.sas.iom.SASIOMDefs.StringSeqHolder;
import com.sas.iom.WorkspaceConnector;
import com.sas.iom.WorkspaceFactory;

import java.util.Properties;

public class ODBCTest {

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

        IWorkspace workspace = connector.getWorkspace();

        ILanguageService sasLanguage = workspace.LanguageService();
//        sasLanguage.Submit("data sasuser.testset;x=1;run;");

//        sasLanguage.Submit("data a;x=1;run;proc print;run;");


        long startTime = System.currentTimeMillis();

        sasLanguage.Submit("" +
                "LIBNAME STRESS ODBC DATASRC=STRESS SCHEMA=dbo  USER='scdtpol_sas'  PASSWORD='P@ssw0rd';\n" +
                "proc sql;\n" +
                "\n" +
                "CREATE TABLE STRESS00003 AS \n" +
                "select DOB,CREDIT_LIMIT,CANCEL_CLOSE_DATE from STRESS.AKKI;                \n" +
                "              \n" +
                "quit; " +
                "" +
                "proc print data=STRESS00003(obs=20);" +
                "run;" );

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