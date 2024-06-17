using System.Data;

namespace ETLExample
{
    public class DataValidator
    {
        //Validates the data in the given DataTables.
        public static void ValidateData(DataTable employees, DataTable positions, DataTable salaries)
        {
            CheckForDuplicateEmployees(employees);
            CheckForMissingOrInconsistentData(employees, positions, salaries);
        }

        private static void CheckForDuplicateEmployees(DataTable employees)
        {
            // Check for duplicate employees
            var duplicateEmployees = employees.AsEnumerable()
                .GroupBy(r => new { FirstName = r.Field<string>("FirstName"), LastName = r.Field<string>("LastName") })
                .Where(g => g.Count() > 1)
                .Select(g => g.Key);

            if (duplicateEmployees.Any())
            {
                Console.WriteLine("Duplicate employees found:");
                foreach (var emp in duplicateEmployees)
                {
                    Console.WriteLine($"{emp.FirstName} {emp.LastName}");
                }
            }
        }

        private static void CheckForMissingOrInconsistentData(DataTable employees, DataTable positions, DataTable salaries)
        {
            // Check for missing or inconsistent data in Employees table
            foreach (DataRow row in employees.Rows)
            {
                if (string.IsNullOrEmpty(row["FirstName"].ToString()) ||
                    string.IsNullOrEmpty(row["LastName"].ToString()) ||
                    string.IsNullOrEmpty(row["Gender"].ToString()))
                {
                    Console.WriteLine("Incomplete employee record found:");
                    Console.WriteLine($"FirstName: {row["FirstName"]}, MiddleInitial: {row["MiddleInitial"]}, LastName: {row["LastName"]}, Gender: {row["Gender"]}");
                }
            }

            // Check for missing or inconsistent data in Positions table
            foreach (DataRow row in positions.Rows)
            {
                if (string.IsNullOrEmpty(row["Year"].ToString()) ||
                    string.IsNullOrEmpty(row["Name"].ToString()) ||
                    string.IsNullOrEmpty(row["PositionTitle"].ToString()))
                {
                    Console.WriteLine("Incomplete position record found:");
                    Console.WriteLine($"Year: {row["Year"]}, Name: {row["Name"]}, PositionTitle: {row["PositionTitle"]}");
                }
            }

            // Check for missing or inconsistent data in Salaries table
            foreach (DataRow row in salaries.Rows)
            {
                if (string.IsNullOrEmpty(row["Year"].ToString()) ||
                    string.IsNullOrEmpty(row["Name"].ToString()) ||
                    string.IsNullOrEmpty(row["Salary"].ToString()) ||
                    string.IsNullOrEmpty(row["PayBasis"].ToString()))
                {
                    Console.WriteLine("Incomplete salary record found:");
                    Console.WriteLine($"Year: {row["Year"]}, Name: {row["Name"]}, Salary: {row["Salary"]}, PayBasis: {row["PayBasis"]}");
                }
            }
        }
    }
}
