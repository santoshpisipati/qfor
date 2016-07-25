using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Web.Http;

namespace Quantum_QFOR.Controllers
{
    public class RandomController : ApiController
    {
        private List<Random> values;

        private IEnumerable<Random> getValues()
        {
            List<Random> RandomData = new List<Random>();
            SqlConnection myConnection;
            string connectionString = ConfigurationManager.ConnectionStrings["ConStr1"].ConnectionString;
            using (myConnection = new SqlConnection(connectionString))
            {
                string query = "SELECT Table2.d2, Table1.c1 FROM Table2 INNER JOIN Table1 on Table2.d1 = Table1.d1";
                SqlCommand myCommand = new SqlCommand(query, myConnection);
                SqlDataReader result = myCommand.ExecuteReader();
                if (result.HasRows)
                    while (result.Read())
                    {
                        RandomData = new List<Random>();
                        Random temp = new Random
                        {
                            c1 = int.Parse(result[0].ToString()),
                            d2 = int.Parse(result[1].ToString())
                        };
                        RandomData.Add(temp);
                    }
            }
            return RandomData;
        }
    }

    public class Random
    {
        public int c1 { get; set; }
        public int d2 { get; set; }
    }
}