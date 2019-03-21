using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;


namespace ServiceDBMesaRPG.Models
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

        public IEnumerable<Usuario> All
        {
            get
            {
                return _usuarios;
            }
        }

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
    }
}