using DAL;
using DTO;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BLL
{
    public class USER_BLL
    {
        public DataTable ObterUser(int cdUser)
        {
            return new USER_DAL().ObterUsuario(cdUser);
        }

        public DataTable ObterUser(string nomeUser)
        {
            return new USER_DAL().ObterUsuario(nomeUser);
        }

        public DataTable ObterUserByEmail(string email)
        {
            return new USER_DAL().ObterUsuarioPorEmail(email);
        }

        public DataTable ObterUser()
        {
            return new USER_DAL().ObterUsuario();
        }

        public void SaveDados(TB_USER objUser)
        {
            new USER_DAL().SaveUser(objUser);
        }

        public void UpdateUser(TB_USER objUser)
        {
            new USER_DAL().UpdateUser(objUser);
        }
    }
}
