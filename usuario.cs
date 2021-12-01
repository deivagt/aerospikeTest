using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AeroSpike_Test
{
    class usuario
    {
       public string nombre, apellido, ciudad;
        public int id;
        public usuario(int id, string nombre, string apellido, string ciudad)
        {
            this.id = id;
            this.nombre = nombre;
            this.apellido = apellido;
            this.ciudad = ciudad;

        }
    }
}
