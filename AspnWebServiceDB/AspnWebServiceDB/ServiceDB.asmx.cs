using BLL;
using DTO;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Web.Services;
namespace AspnWebServiceDB
{
    /// <summary>
    /// Summary description for ServiceDB
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
    //[System.Web.Script.Services.ScriptService]
    public class ServiceDB : System.Web.Services.WebService
    {
        [WebMethod]
        public DataTable GetDados()
        {            
            DataTable dt = new USER_BLL().ObterUser();
            return dt;
        }

        [WebMethod]
        public int SaveDados(TB_USER objUser)
        {
            return new USER_BLL().SaveDados(objUser);
        }
    }
}