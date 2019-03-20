using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DTO
{
    public class TB_USER
    {
        private int cD_USER;
        private string nM_USER;
        private string dS_EMAIL;

        public int CD_USER { get =>cD_USER; set => cD_USER = value; }
        public string NM_USER { get => nM_USER; set => nM_USER = value; }
        public string DS_EMAIL { get => dS_EMAIL; set => dS_EMAIL = value; }
    }
}
