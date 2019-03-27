using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Web;

namespace ServiceDB.Models
{
    public class Usuario_DAL
    {
        public IDataReader ObterUsuario(int cdUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT * FROM TB_USUARIO ");
            sb.Append("WHERE CD_USUARIO = ");
            sb.Append(" '"+cdUser+"'");
            IDataReader dr = new DatabaseHelper().GetDataReader(sb);
            return dr;
        }

        public IDataReader ObterUsuario(string nomeUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT * FROM TB_USUARIO ");
            sb.Append("WHERE NM_USUARIO = ");
            sb.Append("'"+nomeUser+"'");
            IDataReader dr = new DatabaseHelper().GetDataReader(sb);
            return dr;
        }

        public IDataReader ObterUsuarioPorEmail(string email)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT * FROM TB_USUARIO ");
            sb.Append("WHERE DS_EMAIL = ");
            sb.Append("'" + email + "'");
            IDataReader dr = new DatabaseHelper().GetDataReader(sb);
            return dr;
        }

        public IDataReader ObterUsuario()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT * FROM TB_USUARIO USUARIO ");
            IDataReader dr = new DatabaseHelper().GetDataReader(sb);
            return dr;
        }

        public IDataReader ObterLogin(Usuario user)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT * FROM TB_USUARIO ");
            sb.Append("WHERE DS_EMAIL = '"+user.DS_EMAIL+"' OR NM_USARIO = '"+user.NM_USUARIO+"'");
            sb.Append(" AND DS_SENHA = '" + user.DS_SENHA + "'");
            IDataReader dr = new DatabaseHelper().GetDataReader(sb);
            return dr;
        }

        public void SaveUser(Usuario objUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("INSERT INTO TB_USUARIO ");
            sb.Append("(NM_USUARIO, DS_EMAIL, DS_SENHA) ");
            sb.Append("VALUES ");
            sb.Append("('" + objUser.NM_USUARIO + "',  '" + objUser.DS_EMAIL + "', )");
            sb.Append("('" + objUser.DS_SENHA + "')");
            new DatabaseHelper().ExecuteScalar(sb);
        }

        public void UpdateUser(Usuario objUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("UPDATE TB_USUARIO ");
            sb.Append("SET[NM_USUARIO] = '" + objUser.NM_USUARIO + "',  ");
            sb.Append("([DS_EMAIL] = '" + objUser.DS_EMAIL + "')");
            sb.Append("WHERE CD_USER = '" + objUser.CD_USUARIO + "' ");

            new DatabaseHelper().ExecuteScalar(sb);
        }

        public void alterPassword(Usuario objUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("UPDATE TB_TB_USUARIO ");
            sb.Append("SET[DS_SENHA] = '" + objUser.DS_SENHA + "',  ");
            sb.Append("WHERE CD_USUARIO = '" + objUser.CD_USUARIO + "' ");

            new DatabaseHelper().ExecuteScalar(sb);
        }

        public void DeleteUser(int cdUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("DELETE FROM TB_USUARIO ");
            sb.Append("WHERE CD_USUARIO = " + cdUser + " ");
            new DatabaseHelper().ExecuteNonQuery(sb);
        }

        public List<Usuario> ListaUsuario(IDataReader dr)
        {
            List<Usuario> USER = new List<Usuario>();
            while (dr.Read())
            {
                USER = new List<Usuario>();
                for (int i = 0; i < dr.FieldCount; i++)
                {
                    Usuario usuario = new Usuario();
                    usuario.CD_USUARIO = Convert.ToInt32(dr["CD_USUARIO"]);
                    usuario.NM_USUARIO = (dr["NM_USUARIO"]).ToString();
                    usuario.DS_EMAIL = (dr["DS_EMAIL"]).ToString();
                    usuario.DS_SENHA = (dr["DS_SENHA"]).ToString();
                    USER.Add(usuario);
                }
            }
            return USER;
        }

        public Usuario CriarUsuario(IDataReader dr)
        {
            Usuario usuario = new Usuario();
            usuario.CD_USUARIO = Convert.ToInt32(dr["CD_USUARIO"]);
            usuario.NM_USUARIO = (dr["NM_USUARIO"]).ToString();
            usuario.DS_EMAIL = (dr["DS_EMAIL"]).ToString();
            usuario.DS_SENHA = (dr["DS_SENHA"]).ToString();
            return usuario;
        }
    }
}