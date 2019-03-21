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
            sb.Append("SELECT* FROM TB_USER USUARIO ");
            sb.Append("WHERE USUARIO.CD_USER");
            sb.Append(cdUser);
            IDataReader dr = new DatabaseHelper().GetDataReader(sb);
            return dr;
        }

        public IDataReader ObterUsuario(string nomeUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT* FROM TB_USER USUARIO ");
            sb.Append("WHERE USUARIO.CD_USER");
            sb.Append(nomeUser);
            IDataReader dr = new DatabaseHelper().GetDataReader(sb);
            return dr;
        }

        public IDataReader ObterUsuarioPorEmail(string email)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT* FROM TB_USER USUARIO ");
            sb.Append("WHERE USUARIO.CD_USER");
            sb.Append(email);
            IDataReader dr = new DatabaseHelper().GetDataReader(sb);
            return dr;
        }

        public IDataReader ObterUsuario()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT* FROM TB_USER USUARIO ");
            IDataReader dr = new DatabaseHelper().GetDataReader(sb);
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
            sb.Append("([DS_EMAIL] = '" + objUser.DS_EMAIL + "')");
            sb.Append("WHERE CD_USER = '" + objUser.CD_USER + "' ");

            new DatabaseHelper().ExecuteScalar(sb);
        }

        public  void DeleteUser(int cdUser)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("DELETE FROM TB_USER ");
            sb.Append("WHERE CD_USER = " + cdUser + " ");
            new DatabaseHelper().ExecuteNonQuery(sb);
        }

        public List<Usuario>ListaUsuario(IDataReader dr)
        {
            List<Usuario> USER = new List<Usuario>();
            while (dr.Read())
            {
                USER = new List<Usuario>();
                for (int i = 0; i < dr.FieldCount; i++)
                {
                    Usuario usuario = new Usuario();
                    usuario.CD_USER = Convert.ToInt32(dr["CD_USER"]);
                    usuario.NM_USER = (dr["NM_USER"]).ToString();
                    usuario.DS_EMAIL = (dr["DS_EMAIL"]).ToString();
                    USER.Add(usuario);
                }               
            }
            return USER;
        }

        public Usuario CriarUsuario(IDataReader dr)
        {
            Usuario usuario = new Usuario();
            usuario.CD_USER = Convert.ToInt32(dr["CD_USER"]);
            usuario.NM_USER = (dr["NM_USER"]).ToString();
            usuario.DS_EMAIL = (dr["DS_EMAIL"]).ToString();
            return usuario;
        }
    }
}