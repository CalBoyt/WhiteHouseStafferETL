using System.Data;


namespace ETLExample
{
    public class DataTransformer
    {
        // Transforms the extracted data into three DataTables: Employees, Positions, and Salaries.
        public static (DataTable employees, DataTable positions, DataTable salaries) TransformData(DataSet dataSet)
        {
            // Get the first table from the DataSet.
            var table = dataSet.Tables[0];

            // Create DataTables for employees, positions, and salaries.
            //Employee Table
            DataTable employees = new("Employees");
            employees.Columns.Add("FirstName");
            employees.Columns.Add("MiddleInitial");
            employees.Columns.Add("LastName");
            employees.Columns.Add("Gender");

            //Positions Table
            DataTable positions = new("Positions");
            positions.Columns.Add("Year");
            positions.Columns.Add("Name");
            positions.Columns.Add("PositionTitle");

            //Salaries Table
            DataTable salaries = new("Salaries");
            salaries.Columns.Add("Year");
            salaries.Columns.Add("Name");
            salaries.Columns.Add("Salary");
            salaries.Columns.Add("PayBasis");

            // Iterate through each row in the source table.
            foreach (DataRow row in table.Rows)
            {
                //Since name format is 'last,first middle', names are extracted and split into parts at the comma after 'last' and space after 'first'.
                //last name is set to index 0, first name is is set to index 1, middle initial is set to an empty string.
                string name = row.Field<string>("name");
                if (string.IsNullOrWhiteSpace(name)) continue;

                var nameParts = name.Split(',', ' ');

                // Skip rows with invalid name formats.
                if (nameParts.Length < 2)
                {
                    Console.WriteLine($"Skipping invalid name: {name}");
                    continue;
                }

                var lastName = nameParts[0];
                var firstName = nameParts[1];
                string middleInitial = string.Empty;

                //If there are three parts in the name, the third part is considered middle name or initial and set to index 2. If it's a name, the first letter of the name is extracted as middle initial, but if only one character, it is set to middle initial.
                if (nameParts.Length > 2)
                {
                    var middlePart = nameParts[2];
                    if (middlePart.Length == 1)
                    {
                        middleInitial = middlePart;
                    }
                    else
                    {
                        middleInitial = middlePart.Substring(0, 1);
                    }
                }

                //Add to Employees table. Check if a row with firstName and lastName exists in the DataTable before adding a new row to ensure no duplicates.
                if (!employees.AsEnumerable().Any(r => r.Field<string>("FirstName") == firstName && r.Field<string>("LastName") == lastName))
                {
                    string gender = row.Field<string>("gender");
                    if (string.IsNullOrWhiteSpace(gender)) continue;

                    employees.Rows.Add(firstName, middleInitial, lastName, gender);
                }

                //Add to Positions table.
                positions.Rows.Add(row["year"], name, row["position_title"]);

                //Add to Salaries table.
                salaries.Rows.Add(row["year"], name, row["salary"], row["pay_basis"]);
            }

            return (employees, positions, salaries);
        }
    }
}

