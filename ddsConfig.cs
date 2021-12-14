using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AeroSpike_Test
{
    class ddsConfig
    {
        //Esta clase contiene la informacion importante de cada DDS
        string nombre;
        string argumentosKey;
        string criterios;

        public ddsConfig( string nombre, string argumentosKey, string criterios)
        {
            this.criterios = criterios;
            
            this.nombre = nombre;
            this.argumentosKey = argumentosKey;
        }

       

        public string getNombre()
        {
            return this.nombre;
        }

        public string getArgs()
        {
            return this.argumentosKey;
        }


        public string getCriterios()
        {
            return this.criterios;
        }

     
        public void setNombre(string nombre)
        {
            this.nombre = nombre;
        }

        public void setArgumentosKey(string argumentosKey)
        {
            this.argumentosKey = argumentosKey;
        }

        public void setCriterios (string criterios)
        {
            this.criterios = criterios;
        }
    }
}
