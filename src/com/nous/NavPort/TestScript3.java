package com.nous.NavPort;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.log4j.PropertyConfigurator;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestScript3 {
	
	

	String URL = "";
	String server_name = "";
	String user_name = "";
	String password = "";
	String database = "";
	String textfilepath = "";
	String exefilepath = "";
	String notepadData = "";
	String query = "";
	
	public static Logger log = Logger.getLogger(TestScript3.class.getName());
//	 PropertyConfigurator.configure("log4j.properties");
	@BeforeTest
	public void loadConfigFile() {
		// TODO Auto-generated method stub
		
		PropertyConfigurator.configure("log4j.properties");
		
		try {
			log.info("Started Execution of Before Test Method.. !!");
			
			// to read url, username, password from configuration file
			Properties configFile = new Properties();

			try {
				System.out.println("==============================================================================================================================================");
				System.out.println("  				 Script Execution Started 										   ");
				System.out.println("==============================================================================================================================================");
				File myFile = new File("config.properties");
				System.out.println("Attempting to read config.properties file from the location : "+ myFile.getCanonicalPath());
				FileInputStream inputStream = new FileInputStream(new File("config.properties"));
				configFile.load(inputStream);
				inputStream.close();

			} catch (FileNotFoundException b) {
				b.printStackTrace();
			}

			URL = configFile.getProperty("URL");
			server_name = configFile.getProperty("server_name");
			database = configFile.getProperty("database");
			user_name = configFile.getProperty("user_name");
			password = configFile.getProperty("password");
			textfilepath = configFile.getProperty("textfilepath");
			exefilepath = configFile.getProperty("exefilepath");
			
			System.out.println("Loading the data from config file Successfull...!!!\n");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		// Triggering exe file to load data in DB.

		try {
			// exefilepath is the path of the executable
			System.out.println("Attempting to execute supplied EXE file from the location : " + exefilepath);
//			Runtime.getRuntime().exec(exefilepath);

		} catch (Exception b) {
			b.printStackTrace();
		}
		log.info("Execution of Before Test Method Finished.. !!\n");
	}

	@Test(description= "It Compares the data of the Notepad text with inserted data in Database" ) 
	public void NavPortDBTest() throws IOException {
		
		log.info("Started Execution of Actual Test Method.. !!");
		// to connect with the sql server database 
		
				String dbURL = "jdbc:sqlserver://" + server_name + ";Initial Catalog="+ database + ";Integrated Security=False;";
				DBConnection db = new DBConnection();
				db.getDBConnection(dbURL, user_name, password);

		// Code to read data from Note Pad
		
		System.out.println("Attempting to read supplied text file from the location : " + textfilepath);
		FileReader fr = new FileReader(textfilepath);
		BufferedReader br = new BufferedReader(fr);
		
		int line_no = 0;
		int matchedlines=0;
		int unmatchedlines=0;
		while ((notepadData = br.readLine()) != null) {
			++line_no;
			
			System.out.println("==============================================================================================================================================");
			System.out.println("\nLine No " + line_no + " of Notepad:: "+ notepadData);

			// Code to read data from DB

			try {

				String tableID = notepadData.substring(0, 2);
				Integer tableno = new Integer(tableID);
				String table_code = " ";
				if (tableno == 2){
					String WBcode = notepadData.substring(2, 3);
					table_code = " and code = '"+ WBcode+"'";
				}else{
					table_code = " ";
				}
				String refTableQuery = "select * from QA_vNextDW_TXWellBoreRaw.dbo.wba091_wellbore_october2014 where table_id="+tableno+table_code+";";
				String ccquery="";
				System.out.println("..............................."+refTableQuery);
				ResultSet rs2=db.runSql(refTableQuery);
				ResultSet reference_table_status=db.runSql(refTableQuery);
//				System.out.println("..............................."+reference_table_status.next());
				if (reference_table_status.next() == true) {
				while (rs2.next()) {
						
					String tableName=rs2.getString("table_name");	
					Integer dbColumnCountValue=new Integer(rs2.getString("total_db_col"));
//					System.out.println("\nNo of columns in DB for table name"+tableName+":: "+dbColumnCountValue);
					
					ArrayList<String> arrayList = new ArrayList<String>();
					for (int i=1;i<=dbColumnCountValue;i++ ){
						
						//to read ColumnName, ColumnPosition, ColumnLength from refTable and split the notepad text data
						String ColumnName = rs2.getString("col"+i);
						int ColumnPosition =  new Integer(rs2.getString("start_pos_col"+i)) ;
						int ColumnLength = new Integer(rs2.getString("len_col"+i));
						
						//substring will consider column position initial with Zero.
						int notepadColumnPosition=ColumnPosition-1;
						String ColumnValue=notepadData.substring(notepadColumnPosition, notepadColumnPosition+ColumnLength);
						
						//Adding elements to sub array get column name from db and respective value in notepad data
						ArrayList<String> subarray = new ArrayList<String>();
						subarray.add(ColumnName);
						subarray.add(ColumnValue);
						arrayList.addAll(subarray);
					}
					
					// Printing all the data of arrayList
//					System.out.println("\nContent of arrayList ::"+arrayList);
					
					// to make condition query which matches the content taken from notepad to the data available in DB
					String conditionQuery="";
					String expectedValuesinDB="";
					for(int j=0;j<arrayList.size();j=j+2){
						
						//matching WhiteSpaces of notepad data with null value as available in DB 
						boolean whiteSpacesCondition=arrayList.get(j+1).matches("\\s+");
						String conditionValue="";
						String expectedValue="";
						if(whiteSpacesCondition){
							conditionValue=" is Null and ";
							expectedValue="= Null ||";
							
						}else{
							conditionValue="='"+arrayList.get(j+1).replaceAll("\\s+", " is Null")+"' and ";
							if (arrayList.get(j+1).trim().length() == 0) {
								conditionValue="='"+arrayList.get(j+1).replaceAll("\\s+", " is Null")+"' and ";
								expectedValue="='"+arrayList.get(j+1).replaceAll("\\s+", " Null")+"' || ";	
							}else{
								conditionValue="='"+arrayList.get(j+1)+"' and ";
								expectedValue="='"+arrayList.get(j+1)+"' || ";
							}
						}
						
						conditionQuery=conditionQuery+arrayList.get(j)+conditionValue;
						expectedValuesinDB=expectedValuesinDB+arrayList.get(j)+expectedValue;
					}
					//final query which gives the no of matched records
					ccquery="select count(*) as Rows_Matched from QA_vNextDW_TXWellBoreRaw.dbo."+tableName+" where "+conditionQuery.substring(0,conditionQuery.length()-5)+";";
//					System.out.println("\nfinal conditionQuery ::"+ccquery);
					System.out.println("Expected Data in DB        :: "+expectedValuesinDB);
					
					
					//to get the actual data in DB
					
					String columnNames="";
					String schemaQuery = "use QA_vNextDW_TXWellBoreRaw;select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = \'"+tableName+"\' and IS_NULLABLE = \'YES\';";
//					System.out.println("schemaQuery   :: "+schemaQuery);
					
					ResultSet rs=db.runSql(schemaQuery);
					while (rs.next())
					{       
						columnNames = columnNames+ rs.getString(4)+"," ;
					}
//					System.out.println("Column Names data:::: "+columnNames);
					
					String actualDataQuery="Select "+columnNames.substring(0, columnNames.length()-1)+" from "+tableName+";";
//					System.out.println("actualDataQuery::"+actualDataQuery);
					
					int rowNo=0;

					ResultSet res = db.runSql(actualDataQuery);
					ResultSetMetaData meta = res.getMetaData();
					
					while (res.next())

					{
						String dbData = "";
						// appending Columns data of DB
						for (int i = 1; i <= meta.getColumnCount(); i++) {
							dbData = dbData +meta.getColumnName(i)+"='" +res.getString(i)+"' || ";
							}
						rowNo++;
						System.out.println("Actual Data in DB at Row"+rowNo+"  :: "+dbData.replaceAll("'null'", "Null"));
					}
					
				}
				
				int matcher=0;
				ResultSet rs3=db.runSql(ccquery);
				while (rs3.next()) {
					 matcher= new Integer(rs3.getString("Rows_Matched"));
				}
				
				System.out.println("Total matched records count value :: "+matcher);
				
				if(matcher==1){
					matchedlines++;
//					System.out.println("\nSource Line No " + line_no + " of Notepad:: "+ notepadData);
					System.out.println("\nComparison Successful and data matched in both text file and DB ");
				}else if(matcher>1){
					matchedlines++;
					System.out.println("\nDuplicate Records exists in Data Base");
				}
				else {
					unmatchedlines++;
					System.out.println("\nComparison Failed for the Line No "+line_no+" of Notepad in DB ");
//					System.out.println("Note Pad Data :: "+notepadData);
//					System.out.println("Expected Data in DB "+expec);
					
				}
				System.out.println("==============================================================================================================================================");
				
				}else {
					System.out.println("Reference Table Data not Found to compare the Notepad data with DB Data.");
					continue;
				}

			}

			catch (Exception e)

			{
				e.printStackTrace();
			}

		}
		System.out.println("==============================================================================================================================================");
		System.out.println("  				 Execution Report Summary 										   ");
		System.out.println("==============================================================================================================================================");
		System.out.println("				 Total Lines    : "+line_no+"  	                            ");
		System.out.println("				 Matched Lines  : "+matchedlines+"                        ");
		System.out.println("			         UnMatched Lines: "+unmatchedlines+"                    ");
		System.out.println("=============================================================================================================================================="); 
		br.close();
		log.info("Finished Execution of Actual Test Method.. !!\n");

	}

	@AfterTest
	public void tearDown() throws Exception {
		log.info("Started Execution of After Test Method.. !!");

		// Close DB connection
		DBConnection db=new DBConnection();
		try {
			db.finalize();
			System.out.println("Closing SQL server Connection Succesfull...!!!");
			System.out.println("==============================================================================================================================================");
			System.out.println("  				 Script Execution Finished 										   ");
			System.out.println("==============================================================================================================================================");
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		log.info("Finished Execution of After Test Method.. !!");
		
	}

}
