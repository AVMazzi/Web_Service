using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;


namespace ServiceDB.Models
{
    public class UsuarioRepositorio : IUsuarioRepositorio
    {
        private List<Usuario> _usuarios;
        private IDataReader dr;
        private Usuario _usuario;
        public UsuarioRepositorio()
        {
            InicializaDados();
        }

        private void InicializaDados()
        {
            dr = new Usuario_DAL().ObterUsuario();
            _usuarios = new Usuario_DAL().ListaUsuario(dr);
        }

        IEnumerable<Usuario> All
        {
            get
            {
                return _usuarios;
            }
        }

        IEnumerable<Usuario> IUsuarioRepositorio.All => throw new NotImplementedException();

        public Usuario Find(int CD)
        {
            dr = new Usuario_DAL().ObterUsuario(CD);
            _usuario = new Usuario_DAL().CriarUsuario(dr);
            return _usuario;
        }
        public Usuario FindEmail(string email)
        {
            dr = new Usuario_DAL().ObterUsuarioPorEmail(email);
            _usuario = new Usuario_DAL().CriarUsuario(dr);
            return _usuario;
        }
        public Usuario FindName(string nome)
        {
            dr = new Usuario_DAL().ObterUsuario(nome);
            _usuario = new Usuario_DAL().CriarUsuario(dr);
            return _usuario;
        }

        public bool FindLogin(Usuario user)
        {
            dr = new Usuario_DAL().ObterLogin(user);
            if (dr.FieldCount > 0)
            {
                return true;
            }
            else
            {
                return false;
            }

        }
        public void Insert(Usuario usuario)
        {
            new Usuario_DAL().SaveUser(usuario);
        }
        public void Update(Usuario usuario)
        {
            new Usuario_DAL().UpdateUser(usuario);
        }
        public void Delete(int CD)
        {
            new Usuario_DAL().DeleteUser(CD);
        }

        Usuario IUsuarioRepositorio.Find(int CD)
        {
            throw new NotImplementedException();
        }

        Usuario IUsuarioRepositorio.FindEmail(string email)
        {
            throw new NotImplementedException();
        }

        Usuario IUsuarioRepositorio.FindName(string nome)
        {
            throw new NotImplementedException();
        }

        bool IUsuarioRepositorio.FindLogin(Usuario user)
        {
            throw new NotImplementedException();
        }

        void IUsuarioRepositorio.Insert(Usuario usuario)
        {
            throw new NotImplementedException();
        }

        void IUsuarioRepositorio.Update(Usuario usuario)
        {
            throw new NotImplementedException();
        }

        void IUsuarioRepositorio.Delete(int CD)
        {
            throw new NotImplementedException();
        }
    }
}