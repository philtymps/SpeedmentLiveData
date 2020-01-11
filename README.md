# SpeedmentLiveData
Speedment Live Data Client - Works in Conjunction with the Speedment Live Data Agent running on an IBM OMS Instance in the Cloud

Installing:
  download SpeedmentLiveDataClient.7z and extract it to a Folder
  
  The SpeedmentLiveDataClient.jar is a Runnable Jar and startSpeedment.bat is a MS Windows script to strart the Live Data Client Server.  
  
  Database Setup
  You can shose to get a working version of SQLite or an equivalent database installed. Once the database is available:
  1) Obtain your database connection details to use to connect to the client-side database
  2) Edit speedment-livedata-client.properties file
  2) Update the speedment.livedata.database.* parameters to match your database settings. The default setup was done for SQLite
  3) Save the properties file
  
To Run the Windows Client:
  Launch a DOS Prompt switch to the folder whre SpeedmentLiveDataClient is installed and ensure your JAVA_HOME variable points to a valid 1.8.x Version of Java JRE Home and then type:
  
  > Dos Prompt: startSpeedment.bat

This will start the client-side server on your Windows machine in Verbose mode and will load your properties from the properties file you've updated.

To Run the Linux Client:
  Launch a Shell window and switch to the folder where SpeedmentLiveDataClient is installed and ensure your JAVA_HOME variable points to a valid 1.8.x Version of Java JRE Home.  Edit the variables in the speedmentServer.sh file according to where you've installed the client jar and then type:
  
  > ./startSpeedment.sh

This will start the client-side server on your Windows/Linux machine and will load your properties from the properties file you've updated.

Running the Agents:

Launch the OMS BDA Home page: http://diab191.tympsnet.com
From the top menu select Agents and then select DataExtractAgent on the Agents List Screen

This will bring you to the Live Data Agent Page.  Click the green Start Agent button and make sure the agent is started with not errors showing up in the log file.  Now you're ready to test drive the agent.

1) Trigger the SPEEDMENT-RESET transaction.  If successful this will create the database schema for all the tables that need to be created on the target database.  If this doesn't get created NOTHING ELSE WILL WORK!

2) Trigger the SPEEDMENT-ITEMS transaction.  This will extract the YFS_ITEM table from the OMS system and insert into the target database.

3) Trigger the SPEEDMENT-INVENTORY transaction.  This will extract the YFS_INVENTORY_ITEM, YFS_INVENTORY_SUPPLY, and YFS_INVENTORY_DEMAND tables into the target database.

4) Trigger the SPEEDMENT-ORDERS transaction. This will extract the YFS_ORDER_HEADER and YFS_ORDER_LINE tables and insrt into the target database.

To create the database schema on your target database for the first time, manually trigger the SPEEDMENT-RESET agent.  You should notice 6 Tables being created including:
1) YFS_ORDER_HEADER - Order Header Records
2) YFS_ORDER_LINE - Order Line Records
3) YFS_ITEM - Item Records
4) YFS_INVENTORY_ITEM - Inventory Item Records
5) YFS_INVENTORY_SUPPLY - Inventory Supply Records
6) YFS_INVENTORY_DEMAND - Inventory Demand Records

Note you can trigger this transaction anytime you want to flush your target database of all the data and reload the data.  The data loaded is all items from the last 10 years and all your orders and inventory from the prior 30 days.




