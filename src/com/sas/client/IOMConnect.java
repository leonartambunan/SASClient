package com.sas.client;

import com.sas.iom.SAS.IWorkspace;
import com.sas.iom.SAS.IWorkspaceHelper;
import com.sas.services.connection.BridgeServer;
import com.sas.services.connection.ConnectionFactoryAdminInterface;
import com.sas.services.connection.ConnectionFactoryConfiguration;
import com.sas.services.connection.ConnectionFactoryInterface;
import com.sas.services.connection.ConnectionFactoryManager;
import com.sas.services.connection.ConnectionInterface;
import com.sas.services.connection.ManualConnectionFactoryConfiguration;
import com.sas.services.connection.Server;


/**
 * Created by Leonar Tambunan on 4/22/2015.
 */
public class IOMConnect {


    public static void main(String[] args) throws Exception {


// identify the IOM server
        String classID = Server.CLSID_SAS;
        String host = "CLO-UAT-SAS";
//        int port = 8561;
        int port = 5660;
        Server server = new BridgeServer(classID, host, port);
        // make a connection factory configuration with the server
        ConnectionFactoryConfiguration cxfConfig = new ManualConnectionFactoryConfiguration(server);
        // get a connection factory manager
        ConnectionFactoryManager cxfManager = new ConnectionFactoryManager();
        // get a connection factory that matches the configuration
        ConnectionFactoryInterface cxf = cxfManager.getFactory(cxfConfig);
        // get the administrator interface
        ConnectionFactoryAdminInterface admin = cxf.getAdminInterface();
        // get a connection
        String userName = "sasdemo";
        String password = "Student1";
        ConnectionInterface cx = cxf.getConnection(userName, password);
        org.omg.CORBA.Object obj = cx.getObject();
        IWorkspace iWorkspace = IWorkspaceHelper.narrow(obj);
//        <insert iWorkspace workspace usage code here>
        cx.close();

    }
}