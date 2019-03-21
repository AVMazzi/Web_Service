using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ServiceDBMesaRPG.Models
{
    public interface IUsuarioRepositorio
    {
        IEnumerable<Usuario> All { get; }
        Usuario Find(int CD);
        Usuario FindEmail(string email);
        Usuario FindName(string nome);
        void Insert(Usuario usuario);
        void Update(Usuario usuario);
        void Delete(int CD);

    }
}
