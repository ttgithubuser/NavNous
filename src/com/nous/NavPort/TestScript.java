package com.nous.NavPort;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.Properties;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestScript {

	String URL = "";
	String server_name = "";
	String user_name = "";
	String password = "";
	String database = "";
	String textfilepath = "";
	String exefilepath = "";
	String notepadData = "";
	String query = "";

	@BeforeTest
	public void loadConfigFile() {
		// TODO Auto-generated method stub
		try {

			// to read url, username, password from configuration file
			Properties configFile = new Properties();

			try {
				// configFile.load(TestScript.class.getClassLoader().getResourceAsStream("config.properties"));
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

		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		System.out.println("Loading data from config file Successfull...!!!\n");
	}

	@Test
	public void test() throws IOException {
		
		System.out.println("URL:: " + URL);
		
		String dbURL1 = "jdbc:sqlserver://" + server_name + ";Initial Catalog="+ database + ";Integrated Security=False;";

		DBConnection db = new DBConnection();
		db.getDBConnection(dbURL1, user_name, password);
		
		// Triggering exe file to load data in DB.

		try {
			// scriptPath is the path of the executable
			
			System.out.println("exe file canExecute?? ::" + new File(exefilepath).canExecute());
			System.out.println("exe file canRead?? ::" + new File(exefilepath).canRead());
			
			System.out.println("Attempting to execute NavPort.ETL.Texas.ImportWellBoreRaw.exe file from the location : " + exefilepath);
//			Runtime.getRuntime().exec(exefilepath);
			
			File f=new File(exefilepath);
			System.out.println("Abs path ::"+f.getAbsolutePath());
			Process p = Runtime.getRuntime().exec(f.getAbsolutePath());
			
//			ProcessBuilder builder = new ProcessBuilder(new String[] { "cmd.exe", "/C", exefilepath });
			
//			Runtime.getRuntime().exec("C:\\windows\\system32\\calc.exe");

		} catch (Exception b) {
			b.printStackTrace();
		}
		
		
		


		// Code to read data from Note Pad
		
		System.out.println("Attempting to read dbf900.txt file from the location : " + textfilepath);
		FileReader fr = new FileReader(textfilepath);
		BufferedReader br = new BufferedReader(fr);

		int line_no = 0;
		while ((notepadData = br.readLine()) != null) {
			++line_no;
			System.out.println("\nLine No " + line_no + " of Notepad:: "+ notepadData);

			// Code to read data from DB

			try {

				int columnCount = 0;
				String tableID = notepadData.substring(0, 2);
				// System.out.println("tableID :: "+tableID);
				Integer tableno = new Integer(tableID);

				switch (tableno) {
				case 01:
					query = "SELECT RRCTAPERECORDID,WBAPICounty,WBAPIUnique,WBNxtAvailSuffix,WBNxtAvailHoleChgeNbr,WBFieldDistrict,WBResCntyCode,WBOrigComplCC,WBOrigComplCent,WBOrigComplYY,WBOrigComplMM,WBOrigComplDD,WBTotalDepth,WBValidFluidLevel,WBCertRevokedCC,WBCertRevokedYY,WBCertRevokedMM,WBCertRevokedDD,WBCertificationDenialCC,WBCertificationDenialYY,WBCertificationDenialMM,WBCertificationDenialDD,WBDenialReasonFlag,WBErrorAPIAssigneCode,WBReferCorrectAPINbr,WBDummyAPINumber,WBDateDummyReplaced,WBNewestDrlPmtNbr,WBCancelExpireCode,Filler,WBExcept13A,WBFreshWaterFlag,WBPlugFlag,WBPreviousAPINbr,WBCompletionDataInd,WBHistDateSourceFlag,Filler2,WBEx14B2Count,WBDesignationHB1975Flag,WBDesignationEffecCC,WBDesignationEffecYY,WBDesignationEffecMM,WBDesignationRevisedCC,WBDesignationRevisedYY,WBDesignationRevisedMM,WBDesignationLetterCC,WBDesignationLetterYY,WBDesignationLetterMM,WBDesignationLetterDD,WBCertificationEffectCC,WBCertificationEffectYY,WBCertificationEffectMM,WBWaterLandCode,WBTotalBondedDepth,WBOverrideEstPlugCost,WBShutInYear,WBShutInMonth,WBOverrideBondedDepth,WBSubjTo14B2Flag FROM [nous_TXWellBoreRaw].[dbo].[WellBoreTechnicalDataRootSegment]";
					columnCount = 59;
					break;
				case 02:
					query = "SELECT RRCTAPERECORDID,WBOilCode,WBOilDist,WBOilLseNbr,WBOilWellNbr,WBGasCode,WBGasRrcID,WBGasFiller,WBGasDist,WBGasWellNo,WBMultiWellRecNbr,WBAPISuffix,Filler1,WBActiveInactiveCode,Filler2,WBDwnHoleCommingleCode,Filler3,WBCreatedFromPIFlag,WBRule37Nbr,Filler4,Filler5,Filler6,Filler7,Filler8,WBP15,WBP12,WBPlugDatePointer FROM [nous_TXWellBoreRaw].[dbo].[WellBoreCompletionInfoSegment]";
					columnCount = 27;
					break;
				case 03:
					query = "SELECT RRCTAPERECORDID,WBFILEKEY,WBFILEDATE,FILLER1,WBEXCEPTRULE11,WBCEMENTAFFIDAVIT,WBG5,WBW12,WBDIRSURVEY,WBW2G1DATE,WBCOMPLCENTURY,WBCOMPLYEAR,WBCOMPLMONTH,WBCOMPLDAY,WBDRLCOMPLDATE,WBPLUGBDEPTH1,WBPLUGBDEPTH2,WBWATERINJECTIONNBR,WBSALTWTRNBR,FILLER2,WBREMARKSIND,WBELEVATION,WBELEVATIONCODE,WBLOGFILERBA FROM [nous_TXWellBoreRaw].[dbo].[WellBoreTechnicalDataFormsFile]";
					columnCount = 24;
					break;
				case 12:
					query = "SELECT RRCTAPERECORDID,WBLEASENAME,WBSECBLKSURVEYLOC,WBWELLLOCMILES,WBWELLLOCDIRECTION,WBWELLLOCNEARESTTOWN,FILLER1,WBDISTFROMSURVEYLINES,WBDISTDIRECTNEARWELL,FILLER2 FROM [nous_TXWellBoreRaw].[dbo].[OldLocationSegment]";
					columnCount = 10;
					break;
				case 13:
					query = "SELECT RRCTAPERECORDID,WBLOCCOUNTY,WBABSTRACT,WBSURVEY,WBBLOCKNUMBER,WBSECTION,WBALTSECTION,WBALTABSTRACT,WBFEETFROMSURSECT1,WBDIRECFROMSURSECT1,WBFEETFROMSURSECT2,WBDIRECFROMSURSECT2,WBWGS84LATITUDE,WBWGS84LONGITUDE  FROM [nous_TXWellBoreRaw].[dbo].[NewLocationSegment]";
					columnCount = 14;
					break;

				default:
					System.out.println("Table ID is not exist ");
					break;
				}

				// Get the contents of nous_TXWellBoreRaw table from DB

				if (query != "") {
					ResultSet res = db.runSql(query);

					int rowcount = 0;
					int matcher=0;

					while (res.next())

					{
						String dbData = "";
						rowcount++;

						// appending Columns data of DB
						for (int i = 1; i <= columnCount; i++) {
							if(res.getString(i) !=null){
							dbData = dbData + res.getString(i);
							}
						}

						// comparing text file data with data from db
						System.out.println("Line No " + rowcount + " of DB:: "+ dbData);
					
						System.out.println("Comparing notpadData:: "+ notepadData.replaceAll("\\s+", "") + "\n          and dbData:: "+ dbData.replaceAll("\\s+", ""));
						if (notepadData.replaceAll("\\s+", "").equals(dbData.replaceAll("\\s+", ""))) {
							matcher++;
						}

					}
					if(matcher==1){
						System.out.println("Comparison Successful and data matched in both text file and DB ");
					}else if(matcher>1){
						System.out.println("Comparison Successful. But., "+matcher+" identical Records Found in DB");
					}
					else {
						System.out.println("Comparison Failed ");
					}

					// System.out.println("rowcount value:: "+rowcount+"\n");
					if (rowcount == 0) {
						System.out.println("No data available in DB");
					}
				}

			}

			catch (Exception e)

			{
				e.printStackTrace();
			}

		}
		br.close();

	}

	@AfterTest
	public void tearDown() throws Exception {

		// Close DB connection
		DBConnection db=new DBConnection();
		try {
			db.finalize();
			System.out.println("\nClosing SQL server Connection Succesfull...!!!\n");
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
