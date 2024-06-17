using Microsoft.Data.SqlClient;
using System.Data;

namespace ETLExample
{

    public class DataLoader
    {
        private readonly string _connectionString;

        //Constructor to initialize the DataLoader with a connection string.
        public DataLoader(string connectionString)
        {
            _connectionString = connectionString;
        }

        //Loads data from DataTables into the SQL database.
        public void LoadData(DataTable employees, DataTable positions, DataTable salaries)
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();

            LoadTable(connection, employees, "Employees");
            LoadTable(connection, positions, "Positions");
            LoadTable(connection, salaries, "Salaries");
        }

        private static void LoadTable(SqlConnection connection, DataTable table, string tableName)
        {
            foreach (DataRow row in table.Rows)
            {
                using var command = new SqlCommand(GetInsertCommand(table, tableName), connection);
                foreach (DataColumn column in table.Columns)
                {
                    command.Parameters.AddWithValue($"@{column.ColumnName}", row[column]);
                }
                command.ExecuteNonQuery();
            }
        }

        private static string GetInsertCommand(DataTable table, string tableName)
        {
            var columns = string.Join(", ", table.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
            var parameters = string.Join(", ", table.Columns.Cast<DataColumn>().Select(c => $"@{c.ColumnName}"));
            return $"INSERT INTO {tableName} ({columns}) VALUES ({parameters})";
        }
    }
}
