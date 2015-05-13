package com.sas.client;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.sas.metadata.remote.AssociationList;
import com.sas.metadata.remote.CMetadata;
import com.sas.metadata.remote.Column;
import com.sas.metadata.remote.Keyword;
import com.sas.metadata.remote.MdException;
import com.sas.metadata.remote.MdFactory;
import com.sas.metadata.remote.MdFactoryImpl;
import com.sas.metadata.remote.MdOMIUtil;
import com.sas.metadata.remote.MdOMRConnection;
import com.sas.metadata.remote.MdObjectStore;
import com.sas.metadata.remote.MetadataObjects;
import com.sas.metadata.remote.PhysicalTable;
import com.sas.metadata.remote.PrimaryType;
import com.sas.metadata.remote.Tree;


/**
 * This is a test class that contains the examples for SAS Java Metadata Interface.
 */
public class MetaDataTest
{

    /**
     * The object factory instance.
     */
    private MdFactory _factory = null;

    /**
     * Default constructor
     */
    public MetaDataTest()
    {
        // Call the factory's constructor.
        initializeFactory();
    }

    private void initializeFactory()
    {
        try
        {
            // Initialize the factory.  The boolean parameter is used to determine if
            // the application is running in a remote or local environment.  If the
            // data does not need to be accessible across remote JVMs, then
            // "false" can be used, as shown here.
            _factory = new MdFactoryImpl(false);

            // Defines debug logging, but does not turn it on.
            boolean debug = false;
            if (debug)
            {
                _factory.setDebug(false);
                _factory.setLoggingEnabled(false);

                // Sets the output streams for logging.  The logging output can be
                // directed to any OutputStream, including a file.
                _factory.getUtil().setOutputStream(System.out);
                _factory.getUtil().setLogStream(System.out);
            }

            // To be notified when changes have been persisted to the SAS Metadata Server
            // within this factory (this includes adding objects, updating objects, and
            // deleting objects), we can add a listener to the factory here.
            // See MdFactory.addMdFactoryListener()
            // A listener is not needed for this example.
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    /**
     * The following statements make a connection to the SAS Metadata Server
     * and check exceptions if there is an error connecting.  The server name,
     * port, user, and password variables must be substituted with actual values.
     * @return true if the connection was successful.
     */
    public boolean connectToServer()
    {
        String serverName = "CLO-UAT-SAS";
        String serverPort = "8561";
        String serverUser = "sasdemo";
        String serverPass = "Student1";

        try
        {
            MdOMRConnection connection = _factory.getConnection();

            // This statement makes the connection to the server.
            connection.makeOMRConnection(serverName, serverPort, serverUser, serverPass);

            // The following statements define error handling and error
            // reporting messages.
        }
        catch (MdException e)
        {
            Throwable t = e.getCause();
            if (t != null)
            {
                String ErrorType = e.getSASMessageSeverity();
                String ErrorMsg = e.getSASMessage();
                if (ErrorType == null)
                {
                    // If there is no SAS server message, write a Java/CORBA message.
                }
                else
                {
                    // If there is a message from the server:
                    System.out.println(ErrorType + ": " + ErrorMsg);
                }
                if (t instanceof org.omg.CORBA.COMM_FAILURE)
                {
                    // If there is an invalid port number or host name:
                    System.out.println(e.getLocalizedMessage());
                }
                else if (t instanceof org.omg.CORBA.NO_PERMISSION)
                {
                    // If there is an invalid user ID or password:
                    System.out.println(e.getLocalizedMessage());
                }
            }
            else
            {
                // If we cannot find a nested exception, get message and print.
                System.out.println(e.getLocalizedMessage());
            }
            // If there is an error, print the entire stack trace.
            e.printStackTrace();
            return false;
        }
        catch (RemoteException e)
        {
            // Unknown exception.
            e.printStackTrace();
            return false;
        }
        // If no errors occur, then a connection is made.
        return true;
    }

    /**
     * The following statements get and display the status and version
     * of the SAS Metadata Server.
     */
    public void displayServerInformation()
    {
        try
        {
            MdOMRConnection connection = _factory.getConnection();

            // Check the status of the server.
            System.out.println("\nGetting server status...");
            int status = connection.getServerStatus();
            switch (status)
            {
                case MdOMRConnection.SERVER_STATUS_OK:
                    System.out.println("Server is running");
                    break;
                case MdOMRConnection.SERVER_STATUS_PAUSED:
                    System.out.println("Server is paused");
                    break;
                case MdOMRConnection.SERVER_STATUS_ERROR:
                    System.out.println("Server is not running");
                    break;
            }

            // Check the version of the server.
            int version = connection.getPlatformVersion();
            System.out.println("Server version: " + version);
        }
        catch (MdException e)
        {
            e.printStackTrace();
        }
        catch (RemoteException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * The following statements get information about the foundation repository.
     * @return the foundation repository
     */
    public CMetadata getFoundationRepository()
    {
        try
        {
            System.out.println("\nGetting the Foundation repository...");

            // The getFoundationRepository method gets the foundation repository.
            return _factory.getOMIUtil().getFoundationRepository();
        }
        catch (MdException e)
        {
            e.printStackTrace();
        }
        catch (RemoteException e)
        {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * The following statements list the repositories that are registered
     * on the SAS Metadata Server.
     * @return the list of available repository (list of CMetadata objects)
     */
    public List<CMetadata> getAllRepositories()
    {
        try
        {
            System.out.println("\nThe repositories contained on this SAS Metadata " +
                    "Server are:");

            // The getRepositories method lists all repositories.
            MdOMIUtil omiUtil = _factory.getOMIUtil();
            List<CMetadata> reposList = omiUtil.getRepositories();
            for (CMetadata repository : reposList)
            {
                // Print the name and id of each repository.
                System.out.println("Repository: " +
                        repository.getName()
                        + " (" + repository.getFQID() +")");
            }
            return reposList;
        }
        catch (MdException e)
        {
            e.printStackTrace();
        }
        catch (RemoteException e)
        {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    /**
     * The following statements list the metadata types available on the
     * SAS Metadata Server and their descriptions.
     */
    public void displayMetadataTypes()
    {
        try
        {
            System.out.println("\nThe object types contained on this SAS Metadata " +
                    "Server are:");

            // Metadata types are listed with the getTypes method.
            List<String> nameList = new ArrayList<String>();
            List<String> descList = new ArrayList<String>();
            _factory.getOMIUtil().getTypes(nameList, descList);
            Iterator<String> nameIter = nameList.iterator();
            Iterator<String> descIter = descList.iterator();
            while (nameIter.hasNext() && descIter.hasNext())
            {
                // Print the name and description of each metadata object type.
                String name = nameIter.next();
                String desc = descIter.next();
                System.out.println("Type: " +
                        name +
                        " - Description: " +
                        desc);
            }
        }
        catch (MdException e)
        {
            e.printStackTrace();
        }
        catch (RemoteException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * The following statements create a table, column, and keyword on the column.
     * The objects are created in the user's My Folder folder.
     * @param repository CMetadata object with id of form:  A0000001.A5KHUI98
     */
    public void createTable(CMetadata repository)
    {
        if (repository != null)
        {
            try
            {
                System.out.println("\nCreating objects on the server...");

                // We have a repository object.
                // We use the getFQID method to get its fully qualified ID.
                String reposFQID = repository.getFQID();

                // We need the short repository ID to create an object.
                String shortReposID = reposFQID.substring(reposFQID.indexOf('.') + 1,
                        reposFQID.length());

                // Now we create an object store to hold our objects.
                // This will be used to maintain a list of objects to persist
                // to the SAS Metadata Server.
                MdObjectStore store = _factory.createObjectStore();
                String tableName = "TableTest";
                String tableType = "Table";

                // The getUserHomeFolder method retrieves (or creates, if necessary)
                // the user's My Folder folder.  If the folder does not
                // exist, the method automatically creates it.
                // This is the folder in which we will create the table.
                Tree myFolder = _factory.getOMIUtil().getUserHomeFolder(store, "",
                        MdOMIUtil.FOLDERTYPE_MYFOLDER, "", 0, true);

                // Before creating any objects, we must verify that the Table does
                // not already exist within the parent folder.  The table cannot be
                // created if it is not unique within the folder.
                if (!isUnique(myFolder, tableName, tableType))
                {
                    // Create a PhysicalTable object named "TableTest".
                    PhysicalTable table = (PhysicalTable) _factory.createComplexMetadataObject
                            (store,
                                    null,
                                    "TableTest",
                                    MetadataObjects.PHYSICALTABLE,
                                    shortReposID);

                    // Set the PublicType and UsageVersion attributes for the table.
                    table.setPublicType("Table");
                    table.setUsageVersion(1000000.0);

                    // Add the table to the user's "My Folder" location.
                    table.getTrees().add(myFolder);

                    // Create a Column named "ColumnTest".
                    Column column = (Column) _factory.createComplexMetadataObject
                            (store,
                                    null,
                                    "ColumnTest",
                                    MetadataObjects.COLUMN,
                                    shortReposID);

                    // Set the attributes of the column, including PublicType and
                    // UsageVersion.
                    column.setPublicType("Column");
                    column.setUsageVersion(1000000.0);
                    column.setColumnName("MyTestColumnName");
                    column.setSASColumnName("MyTestSASColumnName");
                    column.setDesc("This is a description of a column");

                    // Use the get"AssociationName"() method to associate the column with
                    // the table. This method creates an AssociationList object for the table
                    // object. The inverse association will be created automatically.
                    // The add(MetadataObject) method adds myColumn to the AssociationList.
                    table.getColumns().add(column);

                    // Create a keyword for the column named "KeywordTest".
                    Keyword keyword = (Keyword) _factory.createComplexMetadataObject
                            (store,
                                    null,
                                    "KeywordTest",
                                    MetadataObjects.KEYWORD,
                                    shortReposID);

                    // Associate the keyword with the column.
                    column.getKeywords().add(keyword);

                    // Now, persist all of these changes to the server.
                    table.updateMetadataAll();
                }

                // When finished, clean up the objects in the store if they
                // are no longer being used.
                store.dispose();
            }
            catch (MdException e)
            {
                e.printStackTrace();
            }
            catch (RemoteException e)
            {
                e.printStackTrace();
            }
        }
    }

    /**
     * isUnique() is a private method that determines whether an object is unique
     * within a given folder.
     * @param folder is the name of the parent folder
     * @param name is the Name value of the object
     * @param type is the PublicType value of the object
     * @return true if an object with the specified name and type exists within
     * the folder.
     */
    private boolean isUnique(Tree folder, String name, String type)
            throws RemoteException, MdException
    {
        // Now, retrieve the objects in the folder and make sure that the folder doesn't
        // already contain this table. The object's Name and PublicType attribute values
        //  are used to determine if it is unique.
        List members = folder.getMembers();
        for (Iterator iter = members.iterator(); iter.hasNext(); )
        {
            CMetadata meta = (CMetadata) iter.next();
            if (meta instanceof PrimaryType)
            {
                // Verify that the types and object names match
                // A case-insensitive match should be used when comparing the names.
                if (type.equals(((PrimaryType) meta).getPublicType()) &&
                        name.equals(meta.getName()))
                {
                    // We found a match.
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * The following statements read the newly created objects back from the
     * SAS Metadata Server.
     * @param repository identifies the repository from which to read our objects.
     */
    public void readTable(CMetadata repository)
    {
        if(repository != null)
        {
            try
            {
                System.out.println("\nReading objects from the server...");

                // First we create an MdObjectStore as a container for the
                // objects that we will create/read/persist to the server as
                // one collection.
                MdObjectStore store = _factory.createObjectStore();

                // The following statements define variables used within the
                // getMetadataObjectsSubset method. These XML strings are used
                // with SAS Open Metadata Interface flags. The <XMLSELECT> element
                // specifies filter criteria. The objects returned are filtered by the
                // PublicType and Name values. The <TEMPLATES> element specifies the
                // associations to be expanded for each object.

                String xmlSelect = "<XMLSELECT Search=\"*[@PublicType='Table' and " +
                        "@Name='TableTest']\"/>";
                String template =
                        "<Templates>" +
                                "<PhysicalTable>" +
                                "<Columns/>" +
                                "</PhysicalTable>" +
                                "<Column>" +
                                "<Keywords/>" +
                                "</Column>" +
                                "</Templates>";

                // Add the XMLSELECT and TEMPLATES strings together.
                String sOptions = xmlSelect + template;

                // The following statements go to the server with a fully-qualified
                // repository ID and specify the type of object we are searching for
                // (MetadataObjects.PHYSICALTABLE) using the OMI_XMLSELECT, OMI_TEMPLATE,
                // OMI_ALL_SIMPLE, and OMI_GET_METADATA flags. OMI_ALL_SIMPLE specifies
                // to get all simple attributes for all objects that are returned.
                // OMI_GET_METADATA activates the GetMetadata flags in the GetMetadataObjects
                // request.
                //
                // The table, column, and keyword will be read from the server and created
                // within the specified object store.
                int flags = MdOMIUtil.OMI_XMLSELECT | MdOMIUtil.OMI_TEMPLATE |
                        MdOMIUtil.OMI_ALL_SIMPLE | MdOMIUtil.OMI_GET_METADATA;
                List tableList = _factory.getOMIUtil().getMetadataObjectsSubset(store,
                        repository.getFQID(),
                        MetadataObjects.PHYSICALTABLE,
                        flags,
                        sOptions);
                Iterator iter = tableList.iterator();
                while (iter.hasNext())
                {
                    // Print the Name, Id, PublicType, UsageVersion, and ObjPath values
                    // of the table returned from the server. ObjPath is the folder location.
                    PhysicalTable table = (PhysicalTable) iter.next();
                    System.out.println("Found table: " + table.getName() + " (" +
                            table.getId() + ")");

                    System.out.println("\tType: " + table.getPublicType());
                    System.out.println("\tUsage Version: " + table.getUsageVersion());
                    System.out.println("\tPath: " + _factory.getOMIUtil().getObjectPath(store,
                            table, false));

                    // Get the list of columns for this table.
                    AssociationList columns = table.getColumns();
                    for (int i = 0; i < columns.size(); i++)
                    {
                        // Print the Name, Id, PublicType, UsageVersion, Desc, and ColumnName
                        // values for each column associated with the table.
                        Column column = (Column) columns.get(i);
                        System.out.println("Found column: " + column.getName() + " (" +
                                column.getId() + ")");

                        System.out.println("\tType: " + column.getPublicType());
                        System.out.println("\tUsage Version: " + column.getUsageVersion());
                        System.out.println("\tDescription: " + column.getDesc());
                        System.out.println("\tColumnName: " + column.getColumnName());

                        // Get the list of keywords associated with the columns.
                        AssociationList keywords = column.getKeywords();
                        for (int j = 0; j < keywords.size(); j++)
                        {
                            // Print the Name and Id values of each keyword associated with
                            // the column.
                            Keyword keyword = (Keyword) keywords.get(j);
                            System.out.println("Found keyword: " + keyword.getName() + " (" +
                                    keyword.getId() + ")");
                        }
                    }
                }

                // When finished, clean up the objects in the store if they
                // are no longer being used.
                store.dispose();
            }
            catch (MdException e)
            {
                e.printStackTrace();
            }
            catch (RemoteException e)
            {
                e.printStackTrace();
            }
        }
    }

    /**
     * The following statements delete the objects that we created.
     * @param repository
     */
    public void deleteTable(CMetadata repository)
    {
        if(repository != null)
        {
            try
            {
                System.out.println("\nDeleting the objects from the server...");
                MdObjectStore store = _factory.createObjectStore();

                // Create a list of the objects that need to be deleted
                // from the server.
                List<CMetadata> deleteList = new ArrayList<CMetadata>();

                // Query for the table again.
                String xmlSelect = "<XMLSELECT Search=\"*[@PublicType='Table' and " +
                        "@Name='TableTest']\"/>";

                // Note: Since the object has a valid PublicType value, the SAS 9.3
                // Metadata Server automatically deletes all objects associated
                // with the table, such as its Column and Keyword objects, when the table
                // is deleted. There is no need to specify a template to delete
                // the associated objects.

                int flags = MdOMIUtil.OMI_XMLSELECT;
                List tableList = _factory.getOMIUtil().getMetadataObjectsSubset(store,
                        repository.getFQID(),
                        MetadataObjects.PHYSICALTABLE,
                        flags,
                        xmlSelect);


                // Add the found objects to the delete list.
                Iterator iter = tableList.iterator();
                while (iter.hasNext())
                {
                    PhysicalTable table = (PhysicalTable) iter.next();
                    deleteList.add(table);
                }

                // Delete everything that is in the delete list.
                if (deleteList.size() > 0)
                {
                    System.out.println("Deleting " + deleteList.size() + " objects");
                    _factory.deleteMetadataObjects(deleteList);
                }

                // When finished, clean up the objects in the store if it is no longer
                // being used
                store.dispose();
            }
            catch (MdException e)
            {
                e.printStackTrace();
            }
            catch (RemoteException e)
            {
                e.printStackTrace();
            }
        }
    }

    /**
     * The following statements display the PhysicalTable objects in the repository.
     * @param repository CMetadata identifies the repository from which to read
     * the objects.
     */
    public void displayAllTables(CMetadata repository)
    {
        try
        {
            // Print a descriptive message about the request.
            System.out.println("\nRetrieving all PhysicalTable objects contained in " +
                    " repository " + repository.getName());

            // Use the short repository ID to pass in the method.
            String reposID = repository.getFQID();

            // We get a list of PhysicalTable objects.
            MdObjectStore store = _factory.createObjectStore();

            // Use the OMI_ALL_SIMPLE flag to get all attributes for each table
            // that is returned.
            int flags = MdOMIUtil.OMI_GET_METADATA | MdOMIUtil.OMI_ALL_SIMPLE;
            List tables = _factory.getOMIUtil().getMetadataObjectsSubset
                    (store,
                            reposID,                         // Repository to search
                            MetadataObjects.PHYSICALTABLE,   // Metadata type to search for
                            flags,
                            "" );

            // Print information about them.
            Iterator iter = tables.iterator();
            while( iter.hasNext())
            {
                PhysicalTable ptable = (PhysicalTable)iter.next();
                System.out.println("PhysicalTable: " +
                        ptable.getName() +
                        ", " +
                        ptable.getFQID() +
                        ", " +
                        ptable.getDesc());
            }

            // When finished, clean up the objects in the store if they
            // are no longer being used.
            store.dispose();
        }
        catch (MdException e)
        {
            e.printStackTrace();
        }
        catch (RemoteException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * The following statements retrieve detailed information for a
     * specific PhysicalTable object.
     * @param table the table to retrieve
     */
    public void getTableInformation(PhysicalTable table)
    {
        try
        {
            // Print a descriptive message about the request.
            System.out.println("\nRetrieving information for a specific PhysicalTable");

            // Create a template to retrieve detailed information for this table.
            String template = "<Templates>" +
                    "<PhysicalTable>" +
                    "<Columns/>" +
                    "<Notes/>" +
                    "<Keywords/>" +
                    "</PhysicalTable>" +
                    "</Templates>";

            // Use the OMI_ALL_SIMPLE flag to get all attributes for the table.
            int flags = MdOMIUtil.OMI_GET_METADATA | MdOMIUtil.OMI_ALL_SIMPLE |
                    MdOMIUtil.OMI_TEMPLATE;
            table = (PhysicalTable) _factory.getOMIUtil().getMetadataAllDepths
                    (table,
                            null,
                            null,
                            template,
                            flags);

            // Print information about the table.
            System.out.println("Table attributes: ");
            System.out.println("  Name = " + table.getName());
            System.out.println("  Id = " + table.getId());
            System.out.println("  Description = " + table.getDesc());
            System.out.println("  Created Date = " + table.getMetadataCreated());
            System.out.println("  Type = " + table.getPublicType());
            System.out.println("  Usage Version = " + table.getUsageVersion());
            System.out.println("  Path = " +
                    _factory.getOMIUtil().getObjectPath((MdObjectStore) table.getObjectStore(),
                            table, false));

            System.out.println("Table associations: ");
            System.out.println("  Number of Columns = " + table.getColumns().size());
            System.out.println("  Number of Keywords = " + table.getKeywords().size());
            System.out.println("  Number of Notes = " + table.getNotes().size());
        }
        catch (MdException e)
        {
            e.printStackTrace();
        }
        catch (RemoteException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * The main method for the class
     */
    public static void main(String[] args)
    {
        MetaDataTest tester = new MetaDataTest();

        // connect to the SAS Metadata Server
        boolean connected = tester.connectToServer();
        if(connected)
        {
            System.out.println("Connected...");
        }
        else
        {
            System.out.println("Error Connecting...");
            return;
        }

        tester.displayServerInformation();

        tester.getAllRepositories();

//        tester.displayMetadataTypes();

/*
        CMetadata repos = tester.getFoundationRepository();
        if (repos != null)
        {
            // Create a new PhysicalTable object and add it to the server
            tester.createTable(repos);

            // Query for the PhysicalTable just added to the metadata server
            tester.readTable(repos);

            // Delete the PhysicalTable
            tester.deleteTable(repos);
        }
*/

    }



}
