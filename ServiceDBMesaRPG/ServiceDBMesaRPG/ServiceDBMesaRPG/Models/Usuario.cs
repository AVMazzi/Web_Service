using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace ServiceDBMesaRPG.Models
{
    public class Usuario
    {
        public int CD_USUARIO { get; set; }
        public string NM_USUARIO { get; set; }
        public string DS_EMAIL { get; set; }
        public string DS_SENHA { get; set; }

    }
}