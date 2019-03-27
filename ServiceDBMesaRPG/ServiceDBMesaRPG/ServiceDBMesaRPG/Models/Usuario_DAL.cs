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
            sb.Append("SELECT * FROM TB_USUARIO  ");
            sb.Append("WHERE CD_USUARIO ='" + cdUser + "'");
            DataTable dr = new DatabaseHelper().GetDataTable(sb);
            return dr;
        }

        public DataTable ObterUsuario(string nomeUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT * FROM TB_USUARIO ");
            sb.Append("WHERE NM_USUARIO = '" + nomeUser + "'");
            DataTable dr = new DatabaseHelper().GetDataTable(sb);
            return dr;
        }

        public DataTable ObterUsuarioPorEmail(string email)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT * FROM TB_USUARIO ");
            sb.Append("WHERE DS_EMAIL = '" + email + "'");
            DataTable dr = new DatabaseHelper().GetDataTable(sb);
            return dr;
        }

        public DataTable ObterUsuario()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT* FROM TB_USUARIO");
            DataTable dr = new DatabaseHelper().GetDataTable(sb);
            return dr;
        }

        public DataTable ObterLogin(Usuario usuario)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT* FROM TB_USUARIO ");
            sb.Append("WHERE (DS_EMAIL = '" + usuario.DS_EMAIL + "' OR NM_USUARIO = '" + usuario.NM_USUARIO + "') ");
            sb.Append("AND DS_SENHA = '" + usuario.DS_SENHA + "'");
            DataTable dr = new DatabaseHelper().GetDataTable(sb);
            return dr;
        }

        public void SaveUser(Usuario objUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("INSERT INTO TB_USUARIO ");
            sb.Append("(NM_USUARIO, DS_EMAIL, DS_SENHA) ");
            sb.Append("VALUES ");
            sb.Append("('" + objUser.NM_USUARIO + "',  '" + objUser.DS_EMAIL + "', '" + objUser.DS_SENHA + "')");
            new DatabaseHelper().ExecuteScalar(sb);
        }


        public void UpdateUser(Usuario objUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("UPDATE TB_USUARIO ");
            sb.Append("SET[NM_USUARIO] = '" + objUser.NM_USUARIO + "',  ");
            sb.Append("[DS_EMAIL] = '" + objUser.DS_EMAIL + "' ");
            sb.Append("WHERE CD_USUARIO = '" + objUser.CD_USUARIO + "' ");

            new DatabaseHelper().ExecuteScalar(sb);
        }

        public void DeleteUser(int cdUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("DELETE FROM TB_USUARIO ");
            sb.Append("WHERE CD_USUARIO = '" + cdUser + "' ");
            new DatabaseHelper().ExecuteScalar(sb);
        }

        public List<Usuario> ListaUsuario(DataTable dt)
        {
            List<Usuario> USER = new List<Usuario>();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                Usuario user = new Usuario();
                user.CD_USUARIO = Convert.ToInt32(dt.Rows[i]["CD_USUARIO"]);
                user.NM_USUARIO = dt.Rows[i]["NM_USUARIO"].ToString();
                user.DS_EMAIL = dt.Rows[i]["DS_EMAIL"].ToString();
                USER.Add(user);
            }
            return USER;
        }

        public Usuario CriarUsuario(DataTable dt)
        {
            Usuario user = new Usuario();
            user.CD_USUARIO = Convert.ToInt32(dt.Rows[0]["CD_USUARIO"]);
            user.NM_USUARIO = dt.Rows[0]["NM_USUARIO"].ToString();
            user.DS_EMAIL = dt.Rows[0]["DS_EMAIL"].ToString();
            return user;
        }
    }
}