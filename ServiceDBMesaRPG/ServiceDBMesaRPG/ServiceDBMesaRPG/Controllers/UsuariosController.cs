using ServiceDBMesaRPG.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web.Http;
using System.Net.Http;
using System.Web;
using System.Net;


namespace ServiceDBMesaRPG.Contollers
{
    public class UsuariosController : ApiController
    {
        private readonly IUsuarioRepositorio _usuarioRepositorio;
        public UsuariosController()
        {
            _usuarioRepositorio = new UsuarioRepositorio();
        }

        // GET: api/Usuario
        [HttpGet()]
        public IEnumerable<Usuario> List()
        {
            return _usuarioRepositorio.All;
        }

        // GET: api/Usuario/5
        [HttpGet()]
        public Usuario GetUsuario(int CD)
        {
            var usuario = _usuarioRepositorio.Find(CD);
            if (usuario == null)
            {
                throw new HttpResponseException(new HttpResponseMessage(System.Net.HttpStatusCode.NotFound));
            }
            return usuario;
        }

        // GET: api/Usuario/5
        [HttpGet()]
        public Usuario GetUsuario(string Nome)
        {
            var usuario = _usuarioRepositorio.FindName(Nome);
            if (usuario == null)
            {
                throw new HttpResponseException(new HttpResponseMessage(System.Net.HttpStatusCode.NotFound));
            }
            return usuario;
        }

        // GET: api/Usuario/5
        [HttpGet()]
        public Usuario GetUsuarioEmail(string Email)
        {
            var usuario = _usuarioRepositorio.FindEmail(Email);
            if (usuario == null)
            {
                throw new HttpResponseException(new HttpResponseMessage(System.Net.HttpStatusCode.NotFound));
            }
            return usuario;
        }

        // POST: api/Usuario
        [HttpPost()]
        public void Post([FromBody]Usuario usuario)
        {
            _usuarioRepositorio.Insert(usuario);
        }

        // PUT: api/Usuario/5
        [HttpPut()]
        public void Put([FromBody]Usuario usuario)
        {
            _usuarioRepositorio.Update(usuario);
        }

        // DELETE: api/Usuario/5
        [HttpDelete()]
        public void Delete([FromBody]Usuario usuario)
        {
            _usuarioRepositorio.Delete(usuario.CD_USER);
        }


    }
}