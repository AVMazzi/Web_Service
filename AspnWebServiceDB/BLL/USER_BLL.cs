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

        public DataTable ObterUser()
        {
            return new USER_DAL().ObterUsuario();
        }

        public int SaveDados(TB_USER objUser)
        {
            return new USER_DAL().SaveUser(objUser);
        }
    }
}
