using ExcelDataReader;
using System.Data;

namespace ETLExample
{
    public class DataExtractor
    {
        // Extracts data from an Excel file and returns it as a DataSet.
        public static DataSet ExtractDataFromExcel(string filePath)
        {
            System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);

            // Open the Excel file for reading.
            using (var stream = File.Open(filePath, FileMode.Open, FileAccess.Read))
            {
                using (var reader = ExcelReaderFactory.CreateReader(stream))
                {
                    // Read the Excel data into a DataSet, using the first row as column headers.
                    var result = reader.AsDataSet(new ExcelDataSetConfiguration()
                    {
                        ConfigureDataTable = (_) => new ExcelDataTableConfiguration()
                        {
                            UseHeaderRow = true
                        }
                    });

                    return result;
                }
            }
        }
    }
}
