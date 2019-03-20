using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DTO;

namespace DAL
{
    public class USER_DAL
    {
        public DataTable ObterUsuario(int cdUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT* FROM TB_USER USUARIO ");
            sb.Append("WHERE USUARIO.CD_USER");
            sb.Append(cdUser);
            DataTable dt = new DatabaseHelper().GetDataTable(sb);
            return dt;
        }
        public DataTable ObterUsuario()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT* FROM TB_USER USUARIO ");
            DataTable dt = new DatabaseHelper().GetDataTable(sb);
            return dt;
        }
        public int SaveUser(TB_USER objUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("INSERT INTO TB_USER ");
            sb.Append("VALUES(NM_USER, DS_EMAIL) ");
            sb.Append("'" + objUser.NM_USER + "',  '" + objUser.DS_EMAIL + "'");
            int salvar = int.Parse(new DatabaseHelper().ExecuteScalar(sb.ToString()).ToString());
            return salvar;
        }
    }
}
