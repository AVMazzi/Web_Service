﻿using ServiceDB.Models;
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

        [HttpGet()]
        public bool GetUsuarioLogin(Usuario usuario)
        {
            bool _user = _usuarioRepositorio.FindLogin(usuario);
            return _user;
        }

        [HttpPost()]
        public void Post([FromBody]Usuario usuario)
        {
            _usuarioRepositorio.Insert(usuario);
        }


        [HttpPut()]
        public void Put([FromBody]Usuario usuario)
        {
            //usuario.CD_USER = Cd;
            _usuarioRepositorio.Update(usuario);
        }


        //[Route("api/usuarios/{fileId:int}")]
        //[AcceptVerbs("DELETE")]
        [HttpDelete()]
        public void Delete(int Cd)
        {
            _usuarioRepositorio.Delete(Cd);
        }


    }
}