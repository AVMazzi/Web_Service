using ServiceDB.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web.Http;
using System.Net.Http;
using System.Web.Services;
using System.Net;


namespace ServiceDB.Contollers
{
    public class UsuariosController:ApiController
    {
        private readonly IUsuarioRepositorio _usuarioRepositorio;
        public UsuariosController()
        {
            _usuarioRepositorio = new UsuarioRepositorio();
        }

        [HttpGet]
        public IEnumerable<Usuario> List()
        {
            return _usuarioRepositorio.All;
        }

        public Usuario GetUsuario(int CD)
        {
            var usuario = _usuarioRepositorio.Find(CD);
            if (usuario == null)
            {
                throw new HttpResponseException(new HttpResponseMessage(System.Net.HttpStatusCode.NotFound));
            }
        }


    }
}