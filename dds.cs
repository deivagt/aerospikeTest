using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Aerospike.Client;

namespace AeroSpike_Test
{
    class dds
    {
        string numeroCuenta;
        string ultimaTrans;
        string fechaInsr;

        int totalDiario;
        double montoDiario;
        double higherDiario;
        double lowerDiario;
        double mediaDiario;

        int totalMes;
        double montoMes;
        double higherMes;
        double lowerMes;
        double mediaMes;

        int totalAnio;
        double montoAnio;
        double higherAnio;
        double lowerAnio;
        double mediaAnio;


        string dateTime1;
        string dateTime2;
        string dateTime3;
        string dateTime4;
        string dateTime5;

        List<string> periodTimeV;
        List<int> periodTimeC;
        List<double> periodTimeA;


        public dds(Record miRecord)
        {

           
            foreach(var bin in miRecord.bins)
            {
                switch (bin.Key)
                {
                    case "numeroCuenta":
                        this.numeroCuenta = bin.Value.ToString(); ;
                        break;
                    case "ultimaTrans":
                        this.ultimaTrans = bin.Value.ToString();
                        break;
                    case "fechaInsr":
                        this.fechaInsr = bin.Value.ToString();
                        break;

                    case "totalDiario":
                        this.totalDiario = (int)double.Parse(bin.Value.ToString());
                        break;
                    case "montoDiario":
                        this.montoDiario = double.Parse(bin.Value.ToString());
                        break;
                    case "higherDiario":
                        this.higherDiario = double.Parse(bin.Value.ToString());
                        break;
                    case "lowerDiario":
                        this.lowerDiario = double.Parse(bin.Value.ToString());
                        break;
                    case "mediaDiario":
                        this.mediaDiario = double.Parse(bin.Value.ToString());
                        break;

                    case "totalMes":
                        this.totalMes = (int)double.Parse(bin.Value.ToString());
                        break;
                    case "montoMes":
                        this.montoMes = double.Parse(bin.Value.ToString());
                        break;
                    case "higherMes":
                        this.higherMes = double.Parse(bin.Value.ToString());
                        break;
                    case "lowerMes":
                        this.lowerMes = double.Parse(bin.Value.ToString());
                        break;
                    case "mediaMes":
                        this.mediaMes = double.Parse(bin.Value.ToString());
                        break;

                    case "totalAnio":
                        this.totalAnio = (int)double.Parse(bin.Value.ToString());
                        break;
                    case "montoAnio":
                        this.montoAnio = double.Parse(bin.Value.ToString());
                        break;
                    case "higherAnio":
                        this.higherAnio = double.Parse(bin.Value.ToString());
                        break;
                    case "lowerAnio":
                        this.lowerAnio = double.Parse(bin.Value.ToString());
                        break;
                    case "mediaAnio":
                        this.mediaAnio = double.Parse(bin.Value.ToString());
                        break;


                    case "dateTime1":
                        this.dateTime1 = bin.Value.ToString();
                        break;
                    case "dateTime2":
                        this.dateTime2 = bin.Value.ToString();
                        break;
                    case "dateTime3":
                        this.dateTime3 = bin.Value.ToString();
                        break;
                    case "dateTime4":
                        this.dateTime4 = bin.Value.ToString();
                        break;
                    case "dateTime5":
                        this.dateTime5 = bin.Value.ToString();
                        break;

                    case "periodTimeV":
                        this.periodTimeV = (List<string>)bin.Value;
                        break;
                    case "periodTimeC":
                        this.periodTimeC = (List<int>)bin.Value;
                        break;
                    case "periodTimeA":
                        this.periodTimeA = (List<double>)bin.Value;
                        break;


                    default:
                        
                        Console.WriteLine(bin.Value.ToString());
                        break;
                }
            
            }
        }

        

        public string FNumeroCuenta { get => numeroCuenta; set => numeroCuenta = value; }
        public string FUltimaTrans { get => ultimaTrans; set => ultimaTrans = value; }
        public string FFechaInsr { get => fechaInsr; set => fechaInsr = value; }
        public int FTotalDiario { get => totalDiario; set => totalDiario = value; }
        public double FMontoDiario { get => montoDiario; set => montoDiario = value; }
        public double FHigherDiario { get => higherDiario; set => higherDiario = value; }
        public double FLowerDiario { get => lowerDiario; set => lowerDiario = value; }
        public double FMediaDiario { get => mediaDiario; set => mediaDiario = value; }
        public int FTotalMes { get => totalMes; set => totalMes = value; }
        public double FMontoMes { get => montoMes; set => montoMes = value; }
        public double FHigherMes { get => higherMes; set => higherMes = value; }
        public double FLowerMes { get => lowerMes; set => lowerMes = value; }
        public double FMediaMes { get => mediaMes; set => mediaMes = value; }
        public int FTotalAnio { get => totalAnio; set => totalAnio = value; }
        public double FMontoAnio { get => montoAnio; set => montoAnio = value; }
        public double HigherAnio { get => higherAnio; set => higherAnio = value; }
        public double FLowerAnio { get => lowerAnio; set => lowerAnio = value; }
        public double FMediaAnio { get => mediaAnio; set => mediaAnio = value; }
        public string FDateTime1 { get => dateTime1; set => dateTime1 = value; }
        public string FDateTime2 { get => dateTime2; set => dateTime2 = value; }
        public string FDateTime3 { get => dateTime3; set => dateTime3 = value; }
        public string FDateTime4 { get => dateTime4; set => dateTime4 = value; }
        public string FDateTime5 { get => dateTime5; set => dateTime5 = value; }
        public List<string> FPeriodTimeV { get => periodTimeV; set => periodTimeV = value; }
        public List<int> FPeriodTimeC { get => periodTimeC; set => periodTimeC = value; }
        public List<double> FPeriodTimeA { get => periodTimeA; set => periodTimeA = value; }
    }
}
