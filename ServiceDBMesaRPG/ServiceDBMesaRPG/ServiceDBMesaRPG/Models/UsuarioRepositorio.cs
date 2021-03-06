﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;


namespace ServiceDBMesaRPG.Models
{
    public class UsuarioRepositorio : IUsuarioRepositorio
    {
        private List<Usuario> _usuarios;
        private DataTable dt;
        private Usuario _usuario;
        public UsuarioRepositorio()
        {
            InicializaDados();
        }

        private void InicializaDados()
        {
            dt = new Usuario_DAL().ObterUsuario();
            _usuarios =  new Usuario_DAL().ListaUsuario(dt); ;
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
            dt = new Usuario_DAL().ObterUsuario(CD);
            _usuario = new Usuario_DAL().CriarUsuario(dt);
            return _usuario;
        }
        public Usuario FindEmail(string email)
        {
            dt = new Usuario_DAL().ObterUsuarioPorEmail(email);
            _usuario = new Usuario_DAL().CriarUsuario(dt);
            return _usuario;
        }
        public Usuario FindName(string nome)
        {
            dt = new Usuario_DAL().ObterUsuario(nome);
            _usuario = new Usuario_DAL().CriarUsuario(dt);
            return _usuario;
        }

        public int FindLogin(string usuario, string senha)
        {
            Usuario _usuario = new Usuario();
            _usuario.DS_EMAIL = usuario;
            _usuario.NM_USUARIO = usuario;
            _usuario.DS_SENHA = senha;
            dt = new Usuario_DAL().ObterLogin(_usuario);
            if (dt.Rows.Count > 0)
            {
                return 1;
            }
            else
            {
                return 0;
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
    }
}