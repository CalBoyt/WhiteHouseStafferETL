
using System.Data;

namespace ETLExample
{
    internal class Program
    {
        static void Main(string[] args)
        {
            string excelFilePath = @"C:\Users\calee\workspace\capstone-projects\ETLExample\ETLExample\ExcelFile\whstafferdata.xlsx";
            string connectionString = "server=localhost\\SQLExpress;database=WhiteHouseStaffers;integrated security=true;Trusted_Connection=True;TrustServerCertificate=True;";
            try
            {
                // Extract data from Excel
                DataSet dataSet = DataExtractor.ExtractDataFromExcel(excelFilePath);

                // Transform data
                var (employees, positions, salaries) = DataTransformer.TransformData(dataSet);

                // Validate data
                DataValidator.ValidateData(employees, positions, salaries);

                // Load data into SQL Server
                DataLoader loader = new(connectionString);
                loader.LoadData(employees, positions, salaries);

                Console.WriteLine("ETL process completed successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred: {ex.Message}");
            }
        }
    }
}
