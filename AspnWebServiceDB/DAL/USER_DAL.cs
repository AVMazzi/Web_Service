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

        public DataTable ObterUsuario(string nomeUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT* FROM TB_USER USUARIO ");
            sb.Append("WHERE USUARIO.CD_USER");
            sb.Append(nomeUser);
            DataTable dt = new DatabaseHelper().GetDataTable(sb);
            return dt;
        }

        public DataTable ObterUsuarioPorEmail(string email)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT* FROM TB_USER USUARIO ");
            sb.Append("WHERE USUARIO.CD_USER");
            sb.Append(email);
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

        public void SaveUser(TB_USER objUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("INSERT INTO TB_USER ");
            sb.Append("(NM_USER, DS_EMAIL) ");
            sb.Append("VALUES ");
            sb.Append("('" + objUser.NM_USER + "',  '" + objUser.DS_EMAIL + "')");
            new DatabaseHelper().ExecuteScalar(sb);
        }

        public void UpdateUser(TB_USER objUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("UPDATE TB_USER ");
            sb.Append("SET[NM_USER] = '"+objUser.NM_USER+"',  ");
            sb.Append("([DS_EMAIL] = '" + objUser.DS_EMAIL + "')");
            sb.Append("WHERE CD_USER = '"+objUser.CD_USER+"' ");
            
            new DatabaseHelper().ExecuteScalar(sb);
        }

        public void DeleteUser(int cdUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("DELETE FROM TB_USER ");
            sb.Append("WHERE CD_USER = " + cdUser + " ");
            new DatabaseHelper().ExecuteNonQuery(sb);
        }
    }
}
