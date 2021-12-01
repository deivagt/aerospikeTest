using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AeroSpike_Test
{
    

    class PeriodTime
    {
        string fecha; //formato mm-dd-YYYY
        int contador;
        double monto;

        public PeriodTime(string fecha, int contador, double monto)
        {
            this.fecha = fecha;
            this.contador = contador;
            this.monto = monto;
        }
    }
}
