package com.mycompany.app;

import static java.util.stream.Collectors.toList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.List;

/**
 * JAVA SPARK RECRUITMENT 
 * Candidate: Rui Botelho
 * 
 */
public class App {

	//private static final String file = App.class.getClassLoader().getResource("bank_loan.csv").toString();

	public static void main(String[] args) {
		System.out.println("Start");
		System.setProperty("hadoop.home.dir", "C:\\hadoop");
		
		SparkSession spark = SparkSession.builder().appName("TestRuiBotelho").master("local[*]").getOrCreate();		
		spark.sparkContext().setLogLevel("ERROR");
		
		Dataset<Row> loans = spark.read().option("header", true).csv("C:/bank_loan.csv");
		
		System.out.println("Starting number of rows:" + loans.count());
		System.out.println("Starting the number of columns:" + 	loans.schema().length());

		System.out.println("\nfirst rows of the bank loan data");
		loans.takeAsList(5).forEach(x -> System.out.println(x));
		
		
		loans.createOrReplaceTempView("loans");
		Dataset<Row> loanStatusPossibleValuesDataset = spark.sql("SELECT DISTINCT `Loan Status` FROM loans");
		System.out.println("\nPossible values of column `Loan Status`");
		
		List<String> loanStatusPossibleValues = loanStatusPossibleValuesDataset.collectAsList()
				.stream()
				.map((Row r) -> r.getString(0)).collect(toList());
		loanStatusPossibleValues.stream().forEach(s -> System.out.println(s));
		
		
		loans = loans.drop("Loan ID", "Customer ID");
		
		loans = loans.filter(loans.col("Maximum Open Credit").lt(10000)
				.and(loans.col("Annual Income").lt(7000000)));
		
		loans = loans.filter(loans.col("Current Loan Amount").lt(20000000));
				
		loans = loans.filter((Row r) -> false == r.anyNull());

		System.out.println("\nStarting number of rows:" + loans.count());
		System.out.println("Starting the number of columns:" + 	loans.schema().length());
		System.out.println();
		
		Dataset<Row> nominalLoans = loans.drop("Current Loan Amount","Credit Score","Annual Income","Years in current job",
				"Monthly Debt","Years of Credit History","Months since last delinquent","Number of Open Accounts",
				"Number of Credit Problems","Current Credit Balance","Maximum Open Credit","Bankruptcies","Tax Liens");
		
		Dataset<Row> numericLoans = loans.drop("Loan Status","Term","Home Ownership","Purpose");
		
		ArrayList<Dataset<Row>> partitions = new ArrayList<Dataset<Row>>();
		// partition dataset by Loan Status
		loanStatusPossibleValues.stream().forEach((String loanStatus) -> 
			partitions.add(nominalLoans.filter(col("Loan Status").equalTo(loanStatus))));
		
		partitions.stream().forEach((Dataset<Row> ds ) -> ds.show((int)ds.count()));
		
		
		System.out.println("End");
		
	}

}
