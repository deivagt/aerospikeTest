using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AeroSpike_Test
{
    class ddsConfig
    {
        int id;
        string nombre;
        string argumentosKey;

        public ddsConfig(int id, string nombre, string argumentosKey)
        {
            //numerocuenta:pais::::::
            this.id = id;
            this.nombre = nombre;
            this.argumentosKey = argumentosKey;
        }

        public int getId()
        {
            return this.id;
        }

        public string getNombre()
        {
            return this.nombre;
        }

        public string getArgs()
        {
            return this.argumentosKey;
        }

        public void setId(int id)
        {
            this.id = id;
        }

        public void setNombre(string nombre)
        {
            this.nombre = nombre;
        }

        public void setArgumentosKey(string argumentosKey)
        {
            this.argumentosKey = argumentosKey;
        }
    }
}
