using Aerospike.Client;
using System;
using System.Diagnostics;
using System.IO;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections;

namespace AeroSpike_Test
{
    class Program
    {
        static void Main(string[] args)
        {
            modelado();
            Record record = find(new Key("test","DDS-GLOBAL", "23232323"), new AerospikeClient("127.0.0.1", 3000));

            foreach(var val in record.bins)
            {
                Console.WriteLine(val);
                string[] temp = new string[10];

                if(val.Key == "PeriodTimeV" || val.Key == "PeriodTimeC" || val.Key == "PeriodTimeA")
                {
                    List<object> si = (List<object>)val.Value;
                    int c = 0;
                    
                    foreach (var a in si)
                    {
                        Console.WriteLine(c +" - " + a);
                        c++;
                    }  
                }
            }
        }

        static void modelado()
        {
            //Lista de DDS
            List<ddsConfig> listaDDS = new List<ddsConfig>();

            //DDS a utilizar
            ddsConfig newConfig = new ddsConfig(0, "DDS-GLOBAL", "numeroCuenta");
            listaDDS.Add(newConfig);            

            newConfig = new ddsConfig(1, "DDS-POS", "numeroCuenta");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig(2, "DDS-ATM", "numeroCuenta");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig(3, "DDS-ONLINE", "numeroCuenta");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig(4, "DDS-ATM1", "numeroCuenta:pais");
            listaDDS.Add(newConfig);

            //listaDDS.Add("DDS-POS");
            //listaDDS.Add("DDS-ATM");
            //listaDDS.Add("DDS-ONLINE");

            //Variables de control de acumuladores Dia, Mes, Anio
            bool acumDia = false;
            bool acumMes = false;
            bool acumAnio = false;


            //Conectar a Aerospike
            AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);
            
            //Crear politica de escritura para guardar PK en los Bines
            WritePolicy policy = new WritePolicy();
            policy.sendKey = true;
            policy.SetTimeout(50);

            //Estructuras temporales para la creacion de Bines
            List<String> listaNombresBines = new List<String>();
            List<Bin> listaBines = new List<Bin>();

            //contadores de escritura/lectura
            var timer = new Stopwatch();
            var timerLectura = new Stopwatch();

            //Datos de entrada
            var reader = new StreamReader(File.OpenRead(@"1000trans.csv"));

            //Variables auxiliares
            bool cabecera = true;
            Key key;
            string trama = "";
            bool primero;

            while (!reader.EndOfStream)//Hacer cositas con el csv
            {

                //Partir csv
                var line = reader.ReadLine();
                var values = line.Split(',');

                //Simular trama
                primero = true;
                foreach (var valor in values)
                {
                    if (primero == true)
                    {
                        trama += valor;
                        primero = false;
                    }
                    else
                    {
                        trama += ":" + valor;
                    }
                }

                if (cabecera == true) //Almacenar nombre de bins
                {
                    foreach (var valor in values)
                    {
                        listaNombresBines.Add(valor);
                    }
                    cabecera = false;
                }
                else
                {
                    double i = 0;
                    int contar = 0;
                    foreach (var nombre in listaNombresBines)
                    {
                        if (double.TryParse(values[contar], out i))             //Almacenar valor como doubles/int
                        {
                            listaBines.Add(new Bin(nombre, i));
                        }
                        else
                        {
                            listaBines.Add(new Bin(nombre, values[contar]));    //Almacenar valor como string
                        }
                        contar++;
                    }

                    key = new Key("test", "trxTarjeta", values[0]);

                    //Insertar en SET 
                    timer.Start();
                    client.Put(policy, key, listaBines.ToArray());
                    timer.Stop();

                    /*ALMACENAR DDS GLOBAL - EJECUCION NO DINAMICA------------------------------------------------------*/                   

                    int contador = 0;

                    while (contador < listaDDS.Count)
                    {                       
                        //incrementar para no enciclarnos va
                        contador++;
                        //Limpiar lista de Bines temporales                        
                        listaBines.Clear();
                        //declarar key explicitamente como null para que no se me olvide xd
                        key = null;


                        //Creacion de key condicionada
                        //Debo tener guardada la cabecera del csv para ubicar la posicion de cada una y asi obtener la data

                        List<double> listaPos = new List<double>();

                        string argsActuales = listaDDS[contador - 1].getArgs();

                        bool detener = false;
                        var args = argsActuales.Split(':');

                        foreach(var arg in args)
                        {
                            double pos = obtenerPosicion(listaNombresBines, arg);
                            if(pos == -1)
                            {
                                detener = true;
                                break;
                            }
                            else
                            {
                                listaPos.Add(pos);
                            }
                            
                        }

                       
                        if( detener == true)
                        {
                            break;
                        }

                        string stringKey = "";
                        bool primeraPos = true;

                        foreach(var pos in listaPos)
                        {
                            if(primeraPos == true)
                            {
                                stringKey = values[(int)pos];
                                primeraPos = false;
                            }
                            else
                            {
                                stringKey +=":" + values[(int)pos];
                            }
                            
                        }

                        Console.WriteLine(stringKey);
                        key = new Key("test", listaDDS[contador-1].getNombre(), stringKey);//key variable




                        if ((int.Parse(values[2].ToString()) == (contador-1)) || ((contador - 1) == 0)||(contador-1 >=4))//se escribe o no en dds/siempre en dds global/siempre resto de dds
                        {     
                            
                            //Encontrar bin para decidir si crear bines nuevos o obtener los anteriores
                            bool existe;

                            //funcion para encontrar record
                            Record recordEncontrado = find(key, client);

                            if (recordEncontrado != null)
                            {
                                existe = true;
                            }
                            else
                            {
                                existe = false;
                            }

                            //Generar bines predeterminados
                            if (existe == false)
                            {
                                listaBines.Add(new Bin("ultimaTrx", trama)); 
                                if(acumDia == true)
                                {
                                    listaBines.Add(new Bin("totalDiario", 1));
                                    listaBines.Add(new Bin("montoDiario", double.Parse(values[3])));
                                    listaBines.Add(new Bin("higherDiario", double.Parse(values[3])));
                                    listaBines.Add(new Bin("lowerDiario", double.Parse(values[3])));
                                    listaBines.Add(new Bin("mediaDiario", double.Parse(values[3])));
                                }
                                else
                                {
                                    listaBines.Add(new Bin("totalDiario", 0));
                                    listaBines.Add(new Bin("montoDiario", 0));
                                    listaBines.Add(new Bin("higherDiario", 0));
                                    listaBines.Add(new Bin("lowerDiario", 0));
                                    listaBines.Add(new Bin("mediaDiario", 0));
                                }
                                
                                if(acumMes == true)
                                {
                                    listaBines.Add(new Bin("totalMes", 1));
                                    listaBines.Add(new Bin("montoMes", double.Parse(values[3])));
                                    listaBines.Add(new Bin("higherMes", double.Parse(values[3])));
                                    listaBines.Add(new Bin("lowerMes", double.Parse(values[3])));
                                    listaBines.Add(new Bin("mediaMes", double.Parse(values[3])));
                                }
                                else
                                {
                                    listaBines.Add(new Bin("totalMes", 0));
                                    listaBines.Add(new Bin("montoMes", 0));
                                    listaBines.Add(new Bin("higherMes", 0));
                                    listaBines.Add(new Bin("lowerMes", 0));
                                    listaBines.Add(new Bin("mediaMes", 0));
                                }

                                if (acumAnio == true)
                                {
                                    listaBines.Add(new Bin("totalAnio", 1));
                                    listaBines.Add(new Bin("montoAnio", double.Parse(values[3])));
                                    listaBines.Add(new Bin("higherAnio", double.Parse(values[3])));
                                    listaBines.Add(new Bin("lowerAnio", double.Parse(values[3])));
                                    listaBines.Add(new Bin("mediaAnio", double.Parse(values[3])));
                                }
                                else {
                                    listaBines.Add(new Bin("totalAnio", 0));
                                    listaBines.Add(new Bin("montoAnio", 0));
                                    listaBines.Add(new Bin("higherAnio", 0));
                                    listaBines.Add(new Bin("lowerAnio", 0));
                                    listaBines.Add(new Bin("mediaAnio", 0));
                                }

                                listaBines.Add(new Bin("fechaTrx", values[6].ToString()));
                               
                                listaBines.Add(new Bin("DateTime1", values[6].ToString()));
                                listaBines.Add(new Bin("DateTime2", ""));
                                listaBines.Add(new Bin("DateTime3", ""));
                                listaBines.Add(new Bin("DateTime4", ""));
                                listaBines.Add(new Bin("DateTime5", ""));

                                // Variables para guardar dia mes y año de la transaccion y reiniciar contadores
                                string fechaTrx = values[6].ToString();
                                var valorsFechaTrx = fechaTrx.Split("-");
                                int diaTrx = int.Parse(valorsFechaTrx[0]);
                                int mesTrx = int.Parse(valorsFechaTrx[1]);
                                int anioTrx = int.Parse(valorsFechaTrx[2]);

                                //PeriodTime Vector 
                                string[] arregloFecha = new string[10];                                
                                arregloFecha[0] = values[6].ToString();//Guardar fecha formato ddmmYYYY
                                listaBines.Add(new Bin("PeriodTimeV", arregloFecha));

                                //PeriodTime Count
                                double[] arregloContador = new double[10];
                                arregloContador[0] = 1;//Contador transaccion por dia
                                listaBines.Add(new Bin("PeriodTimeC", arregloContador));

                                //PeriodTime Amount
                                double[] arregloMonto = new double[10];
                                arregloMonto[0] = double.Parse(values[3]);//Monto del dia
                                listaBines.Add(new Bin("PeriodTimeA", arregloMonto));
                            }
                            else // Modificar record
                            {  
                                Bin temporal;
                                double temporalNumerico1;
                                double temporalNumerico2;
                                double temporalNumericoRes;
                                double temporalNumEntrada;
                                //Variables para guardar dia mes y año de la transaccion y reiniciar contadores
                                string fechaTrx = values[6].ToString();
                                var valorsFechaTrx = fechaTrx.Split("-");
                                int diaTrx = int.Parse(valorsFechaTrx[0]);
                                int mesTrx = int.Parse(valorsFechaTrx[1]);
                                int anioTrx = int.Parse(valorsFechaTrx[2]);

                                //fecha del record
                                string fechaInsr = recordEncontrado.GetValue("fechaTrx").ToString();
                                var valoresFechaInsr = fechaInsr.Split('-');
                                int diaInsr = int.Parse(valoresFechaInsr[0]);
                                int mesInsr = int.Parse(valoresFechaInsr[1]);
                                int anioInsr = int.Parse(valoresFechaInsr[2]);

                                //Actualizar ultima trama por defecto
                                listaBines.Add(new Bin("ultimaTrx", trama));

                                //fecha de insercion del bin
                                listaBines.Add(new Bin("fechaTrx", fechaTrx));

                                //Modificar Date y time
                                listaBines.Add(new Bin("DateTime1", fechaTrx));
                                listaBines.Add(new Bin("DateTime2", recordEncontrado.GetValue("DateTime1").ToString()));
                                listaBines.Add(new Bin("DateTime3", recordEncontrado.GetValue("DateTime2").ToString()));
                                listaBines.Add(new Bin("DateTime4", recordEncontrado.GetValue("DateTime3").ToString()));
                                listaBines.Add(new Bin("DateTime5", recordEncontrado.GetValue("DateTime4").ToString()));

                                //fecha de trx en DateTime para comparacion
                                DateTime fechaIngreso = DateTime.Parse(fechaTrx);
                                int contElementos = 0;
                                List<object> listaPeriodVectores = (List<object>)recordEncontrado.GetValue("PeriodTimeV");                                
                                string[] arrayPeriodVectores = new string[10];
                                foreach(var elemento in listaPeriodVectores)
                                {
                                    if(elemento != null)
                                    {
                                        arrayPeriodVectores[contElementos] = elemento.ToString();
                                    }
                                    else
                                    {
                                        arrayPeriodVectores[contElementos] = null;
                                    }
                                   
                                    contElementos++;
                                }

                                contElementos = 0;
                                List<object> listaPeriodCount = (List<object>)recordEncontrado.GetValue("PeriodTimeC");
                                double[] arrayPeriodCount = new double[10];
                                foreach (var elemento in listaPeriodCount)
                                {
                                    if (elemento != null)
                                    {
                                        arrayPeriodCount[contElementos] = (double)elemento;
                                    }
                                    else 
                                    {
                                        arrayPeriodCount[contElementos] = 0;
                                    }

                                        
                                    contElementos++;
                                }
                                contElementos = 0;
                                List<object> listaPeriodAmount = (List<object>)recordEncontrado.GetValue("PeriodTimeA");
                                double[] arrayPeriodAmount = new double[10];
                                foreach (var elemento in listaPeriodAmount)
                                {
                                    if (elemento != null)
                                    {
                                        arrayPeriodAmount[contElementos] = (double)elemento;
                                    }
                                    else
                                    {
                                        arrayPeriodAmount[contElementos] = 0;
                                    }                                        
                                    contElementos++;
                                }
                                

                                //Modificar PeriodTime
                                int c = 0;
                                DateTime fechaTemp;
                                double timeDiff;
                              
                                for(int j = 0;i < 10;i++)
                                {
                                    string elemento = arrayPeriodVectores[j];
                                    if (elemento != null)
                                    {
                                        
                                        fechaTemp = DateTime.Parse(elemento.ToString());
                                        timeDiff = (fechaIngreso-fechaTemp).TotalDays;
                                        if(timeDiff > 0)
                                        {
                                            int distancia = c + (int)timeDiff;
                                          
                                            if(distancia < 10)
                                            {
                                               
                                                arrayPeriodVectores[distancia] = fechaTrx;
                                                arrayPeriodCount[distancia] += 1;
                                                arrayPeriodAmount[distancia] += double.Parse(values[3].ToString());
                                                break;

                                            }
                                            else//Limpiar y escribir
                                            {

                                                int limpiar = 0;
                                                arrayPeriodVectores[0] = fechaTrx;
                                                arrayPeriodCount[0] = 1;
                                                arrayPeriodAmount[0] = double.Parse(values[3].ToString());
                                                limpiar++;
                                                while (limpiar < 10)
                                                {
                                                    arrayPeriodVectores[limpiar] = null;
                                                    arrayPeriodCount[limpiar] = 0;
                                                    arrayPeriodAmount[limpiar] = 0;
                                                    limpiar++;
                                                }
                                                break;
                                            }
                                        }
                                        else
                                        {
                                            
                                            if (timeDiff == 0)
                                            {
                                                arrayPeriodVectores[c] = elemento.ToString();
                                                arrayPeriodCount[c] += 1;
                                                arrayPeriodAmount[c] += double.Parse(values[3].ToString());
                                                break;
                                            }
                                            else
                                            {
                                                //posible bug sin resolver
                                            }   
                                        }
                                    }
                                    c++;
                                }
                               
                                listaBines.Add(new Bin("PeriodTimeV", arrayPeriodVectores));
                                listaBines.Add(new Bin("PeriodTimeC", arrayPeriodCount));
                                listaBines.Add(new Bin("PeriodTimeA", arrayPeriodAmount));

                                foreach (var bin in recordEncontrado.bins)//Identificar nombre del bin en el record encontrado
                                {
                                    switch (bin.Key)
                                    {
                                        /*Acumulador diario*/
                                        
                                        case "totalDiario":                                           
                                            if(acumDia == false) {
                                                break;
                                            }

                                            if(diaTrx > diaInsr)
                                            {
                                                temporal = new Bin("totalDiario", 1);
                                                listaBines.Add(temporal);
                                                break;
                                            }
                                           
                                                temporalNumerico1 = double.Parse(bin.Value.ToString());
                                                temporalNumerico1++;
                                                temporal = new Bin("totalDiario", temporalNumerico1);
                                                listaBines.Add(temporal);                                            
                                            
                                            break;

                                        case "montoDiario":
                                            if (acumDia == false)
                                            {
                                                break;
                                            }

                                            if (diaTrx > diaInsr)
                                            {
                                                listaBines.Add(new Bin("montoDiario", double.Parse(values[3].ToString())));
                                                break;
                                            }
                                            
                                            listaBines.Add(new Bin("montoDiario", double.Parse(values[3])));                                            

                                            break;

                                        case "higherDiario":
                                            if (acumDia == false)
                                            {
                                                break;
                                            }

                                            if (diaTrx > diaInsr)
                                            {
                                                temporal = new Bin("higherDiario", double.Parse(values[3].ToString()));
                                                listaBines.Add(temporal);
                                                break;
                                            }

                                            temporalNumerico1 = double.Parse(bin.Value.ToString());
                                            temporalNumEntrada = double.Parse(values[3].ToString());
                                            if (temporalNumEntrada > temporalNumerico1)
                                            {
                                                temporal = new Bin("higherDiario", temporalNumEntrada);
                                                listaBines.Add(temporal);
                                            }
                                            break;
                                        case "lowerDiario":
                                            if (acumDia == false)
                                            {
                                                break;
                                            }
                                            if (diaTrx > diaInsr)
                                            {
                                                temporal = new Bin("lowerDiario", double.Parse(values[3].ToString()));
                                                listaBines.Add(temporal);
                                                break;
                                            }

                                            temporalNumerico1 = double.Parse(bin.Value.ToString());
                                            temporalNumEntrada = double.Parse(values[3].ToString());
                                            if (temporalNumEntrada < temporalNumerico1)
                                            {
                                                temporal = new Bin("lowerDiario", temporalNumEntrada);
                                                listaBines.Add(temporal);
                                            }
                                            break;
                                        case "mediaDiario":
                                            if (acumDia == false)
                                            {
                                                break;
                                            }

                                            if (diaTrx > diaInsr)
                                            {
                                                temporal = new Bin("mediaDiario", double.Parse(values[3].ToString()));
                                                listaBines.Add(temporal);
                                                break;
                                            }

                                            temporalNumerico1 = double.Parse(recordEncontrado.GetValue("higherDiario").ToString());
                                            temporalNumerico2 = double.Parse(recordEncontrado.GetValue("lowerDiario").ToString());
                                            temporalNumericoRes = (temporalNumerico1 + temporalNumerico2) / 2;
                                            temporal = new Bin("mediaDiario", temporalNumericoRes);
                                            listaBines.Add(temporal);
                                            break;

                                        /*Acumulador Mes*/

                                        case "totalMes":
                                            if(acumMes == false)
                                            {
                                                break;
                                            }

                                            if (mesTrx > mesInsr)
                                            {
                                                temporal = new Bin("totalMes", 1);
                                                listaBines.Add(temporal);
                                                break;
                                            }

                                            temporalNumerico1 = double.Parse(bin.Value.ToString());
                                            temporalNumerico1++;
                                            temporal = new Bin("totalMes", temporalNumerico1);
                                            listaBines.Add(temporal);


                                            break;

                                        case "montoMes":
                                            if (acumMes == false)
                                            {
                                                break;
                                            }
                                            if (mesTrx > mesInsr)
                                            {
                                                listaBines.Add(new Bin("montoMes", double.Parse(values[3].ToString())));
                                                break;
                                            }

                                            listaBines.Add(new Bin("montoMes", double.Parse(values[3])));

                                            break;

                                        case "higherMes":
                                            if (acumMes == false)
                                            {
                                                break;
                                            }
                                            if (mesTrx > mesInsr)
                                            {
                                                temporal = new Bin("higherMes", double.Parse(values[3].ToString()));
                                                listaBines.Add(temporal);
                                                break;
                                            }

                                            temporalNumerico1 = double.Parse(bin.Value.ToString());
                                            temporalNumEntrada = double.Parse(values[3].ToString());
                                            if (temporalNumEntrada > temporalNumerico1)
                                            {
                                                temporal = new Bin("higherMes", temporalNumEntrada);
                                                listaBines.Add(temporal);
                                            }
                                            break;
                                        case "lowerMes":
                                            if (acumMes == false)
                                            {
                                                break;
                                            }
                                            if (mesTrx > mesInsr)
                                            {
                                                temporal = new Bin("lowerMes", double.Parse(values[3].ToString()));
                                                listaBines.Add(temporal);
                                                break;
                                            }

                                            temporalNumerico1 = double.Parse(bin.Value.ToString());
                                            temporalNumEntrada = double.Parse(values[3].ToString());
                                            if (temporalNumEntrada < temporalNumerico1)
                                            {
                                                temporal = new Bin("lowerMes", temporalNumEntrada);
                                                listaBines.Add(temporal);
                                            }
                                            break;
                                        case "mediaMes":
                                            if (acumMes == false)
                                            {
                                                break;
                                            }
                                            if (mesTrx > mesInsr)
                                            {
                                                temporal = new Bin("mediaMes", double.Parse(values[3].ToString()));
                                                listaBines.Add(temporal);
                                                break;
                                            }

                                            temporalNumerico1 = double.Parse(recordEncontrado.GetValue("higherMes").ToString());
                                            temporalNumerico2 = double.Parse(recordEncontrado.GetValue("lowerMes").ToString());
                                            temporalNumericoRes = (temporalNumerico1 + temporalNumerico2) / 2;
                                            temporal = new Bin("mediaMes", temporalNumericoRes);
                                            listaBines.Add(temporal);
                                            break;
                                        /*Acumulador anio*/

                                        case "totalAnio":

                                            if(acumAnio == false)
                                            {
                                                break;
                                            }
                                            if (anioTrx > anioInsr)
                                            {
                                                temporal = new Bin("totalAnio", 1);
                                                listaBines.Add(temporal);
                                                break;
                                            }

                                            temporalNumerico1 = double.Parse(bin.Value.ToString());
                                            temporalNumerico1++;
                                            temporal = new Bin("totalAnio", temporalNumerico1);
                                            listaBines.Add(temporal);


                                            break;

                                        case "montoAnio":
                                            if (acumAnio == false)
                                            {
                                                break;
                                            }
                                            if (anioTrx > anioInsr)
                                            {
                                                listaBines.Add(new Bin("montoAnio", double.Parse(values[3].ToString())));
                                                break;
                                            }

                                            listaBines.Add(new Bin("montoAnio", double.Parse(values[3])));

                                            break;

                                        case "higherAnio":
                                            if (acumAnio == false)
                                            {
                                                break;
                                            }
                                            if (anioTrx > anioInsr)
                                            {
                                                temporal = new Bin("higherAnio", double.Parse(values[3].ToString()));
                                                listaBines.Add(temporal);
                                                break;
                                            }

                                            temporalNumerico1 = double.Parse(bin.Value.ToString());
                                            temporalNumEntrada = double.Parse(values[3].ToString());
                                            if (temporalNumEntrada > temporalNumerico1)
                                            {
                                                temporal = new Bin("higherAnio", temporalNumEntrada);
                                                listaBines.Add(temporal);
                                            }
                                            break;
                                        case "lowerAnio":
                                            if (acumAnio == false)
                                            {
                                                break;
                                            }
                                            if (anioTrx > anioInsr)
                                            {
                                                temporal = new Bin("lowerAnio", double.Parse(values[3].ToString()));
                                                listaBines.Add(temporal);
                                                break;
                                            }

                                            temporalNumerico1 = double.Parse(bin.Value.ToString());
                                            temporalNumEntrada = double.Parse(values[3].ToString());
                                            if (temporalNumEntrada < temporalNumerico1)
                                            {
                                                temporal = new Bin("lowerAnio", temporalNumEntrada);
                                                listaBines.Add(temporal);
                                            }
                                            break;
                                        case "mediaAnio":
                                            if (acumAnio == false)
                                            {
                                                break;
                                            }
                                            if (anioTrx > anioInsr)
                                            {
                                                temporal = new Bin("mediaAnio", double.Parse(values[3].ToString()));
                                                listaBines.Add(temporal);
                                                break;
                                            }

                                            temporalNumerico1 = double.Parse(recordEncontrado.GetValue("higherAnio").ToString());
                                            temporalNumerico2 = double.Parse(recordEncontrado.GetValue("lowerAnio").ToString());
                                            temporalNumericoRes = (temporalNumerico1 + temporalNumerico2) / 2;
                                            temporal = new Bin("mediaAnio", temporalNumericoRes);
                                            listaBines.Add(temporal);
                                            break;


                                        default:
                                            //Console.WriteLine("Whoooops! Something goes wrong :c "+ bin.Key);
                                            break;
                                    }
                                }
                            }

                            //Insertar
                            timer.Start();
                            client.Put(policy, key, listaBines.ToArray());
                            timer.Stop();
                        }


                    }

                    
                }


                key = null;
                listaBines.Clear();
                trama = "";
            }

            TimeSpan timeTaken = timer.Elapsed;
            string resultado = "Tiempo total: " + timeTaken.ToString(@"hh\:mm\:ss\.fff");
            Console.WriteLine(resultado);
        }

        static Record find(Key key, AerospikeClient client)        {
            
            Record record = client.Get(null, key);
            if (record != null)
            {
                return record;
            }
            return null;
        }


        static double obtenerPosicion(List<String> listaNombresBines, string busqueda)
        {
            int i = 0;
            foreach(var nombreBin in listaNombresBines)
            {
                if(nombreBin == busqueda){
                    return i;
                }
                i++;
            }
            Console.WriteLine("bin no encontrado");

            return -1;
        }

        

      
    }
}
