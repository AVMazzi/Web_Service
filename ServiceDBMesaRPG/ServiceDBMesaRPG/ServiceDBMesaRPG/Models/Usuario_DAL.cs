using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Web;

namespace ServiceDBMesaRPG.Models
{
    public class Usuario_DAL
    {
        public DataTable ObterUsuario(int cdUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT * FROM TB_USER  ");
            sb.Append("WHERE CD_USER ='"+cdUser+"'");
            DataTable dr = new DatabaseHelper().GetDataTable(sb);
            return dr;
        }

        public DataTable ObterUsuario(string nomeUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT * FROM TB_USER ");
            sb.Append("WHERE NM_USER = '"+nomeUser+"'");
            DataTable dr = new DatabaseHelper().GetDataTable(sb);
            return dr;
        }

        public DataTable ObterUsuarioPorEmail(string email)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT * FROM TB_USER ");
            sb.Append("WHERE DS_EMAIL = '"+email+"'");
            DataTable dr = new DatabaseHelper().GetDataTable(sb);
            return dr;
        }

        public DataTable ObterUsuario()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT* FROM TB_USER");
            DataTable dr = new DatabaseHelper().GetDataTable(sb);
            return dr;
        }

        public void SaveUser(Usuario objUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("INSERT INTO TB_USER ");
            sb.Append("(NM_USER, DS_EMAIL) ");
            sb.Append("VALUES ");
            sb.Append("('" + objUser.NM_USER + "',  '" + objUser.DS_EMAIL + "')");
            new DatabaseHelper().ExecuteScalar(sb);
        }

        public void UpdateUser(Usuario objUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("UPDATE TB_USER ");
            sb.Append("SET[NM_USER] = '" + objUser.NM_USER + "',  ");
            sb.Append("[DS_EMAIL] = '" + objUser.DS_EMAIL + "' ");
            sb.Append("WHERE CD_USER = '" + objUser.CD_USER + "' ");

            new DatabaseHelper().ExecuteScalar(sb);
        }

        public  void DeleteUser(int cdUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("DELETE FROM TB_USER ");
            sb.Append("WHERE CD_USER = '" + cdUser + "' ");
            new DatabaseHelper().ExecuteScalar(sb);
        }

        public List<Usuario>ListaUsuario(DataTable dt)
        {
            List<Usuario> USER = new List<Usuario>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                Usuario user = new Usuario();
                user.CD_USER = Convert.ToInt32(dt.Rows[i]["CD_USER"]);
                user.NM_USER = dt.Rows[i]["NM_USER"].ToString();
                user.DS_EMAIL = dt.Rows[i]["DS_EMAIL"].ToString();
                USER.Add(user);
            }
            return USER;
        }

        public Usuario CriarUsuario(DataTable dt)
        {
            Usuario user = new Usuario();
            user.CD_USER = Convert.ToInt32(dt.Rows[0]["CD_USER"]);
            user.NM_USER = dt.Rows[0]["NM_USER"].ToString();
            user.DS_EMAIL = dt.Rows[0]["DS_EMAIL"].ToString();
            return user;
        }
    }
}