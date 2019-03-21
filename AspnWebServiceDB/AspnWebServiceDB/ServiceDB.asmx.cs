using BLL;
using DTO;
using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Web.Services;
namespace AspnWebServiceDB
{
    /// <summary>
    /// Summary description for ServiceDB
    /// </summary>
    ////[WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    [WebService(Namespace = "http://microsoft.com/webservices/")]
    // To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
    //[System.Web.Script.Services.ScriptService]
    public class ServiceDB : System.Web.Services.WebService
    {
        [WebMethod]
        public DataTable GetUser()
        {            
            DataTable dt = new USER_BLL().ObterUser();
            return dt;
        }

        [WebMethod]
        public DataTable GetUserName(string nomeUser)
        {
            DataTable dt = new USER_BLL().ObterUser(nomeUser);
            return dt;
        }

        [WebMethod]
        public DataTable GetUserCD(int cdUser)
        {
            DataTable dt = new USER_BLL().ObterUser(cdUser);
            return dt;
        }

        [WebMethod]
        public DataTable GetUserByEmail(string email)
        {
            DataTable dt = new USER_BLL().ObterUserByEmail(email);
            return dt;
        }
        [WebMethod]
        public void SaveUser(TB_USER objUser)
        {
           new USER_BLL().SaveDados(objUser);
        }

        [WebMethod]
        public void UpdateUser(TB_USER objUser)
        {
            new USER_BLL().UpdateUser(objUser);
        }
    }
}