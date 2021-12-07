using Aerospike.Client;
using System;
using System.Diagnostics;
using System.IO;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections;
using Newtonsoft.Json.Converters;

namespace AeroSpike_Test
{
    class Program
    {
        //Propiedades globales
        static List<ddsConfig> listaDDS;
        //contadores de escritura/lectura
        static Stopwatch timer = new Stopwatch();
        static Stopwatch timerLectura = new Stopwatch();
        static void Main(string[] args)
        {

            modelado();

            
                         
        }

        static void modelado()
        {
            //Iniciar lista de DDS
            listaDDS = new List<ddsConfig>();

            //DDS a utilizar
            ddsConfig newConfig;
            newConfig = new ddsConfig( "DDS-GLOBAL", "numeroCuenta", "");
            listaDDS.Add(newConfig);            

            newConfig = new ddsConfig( "DDS-POS", "numeroCuenta","canal=1");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig( "DDS-ATM", "numeroCuenta", "canal=2");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig( "DDS-ONLINE", "numeroCuenta", "canal=3");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig( "DDS-POS-pais", "numeroCuenta:pais", "canal=1");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig( "DDS-ATM-pais", "numeroCuenta:pais", "canal=2");
            listaDDS.Add(newConfig);            

            newConfig = new ddsConfig( "DDS-ONLINE-pais", "numeroCuenta:pais", "canal=3");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig( "DDS-SuperMarket", "numeroCuenta", "mcc=1");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig( "DDS-SuperMarket-pais", "numeroCuenta:pais", "mcc=1");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig( "DDS-Pharmacy", "numeroCuenta", "mcc=2");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig( "DDS-Gasoline", "numeroCuenta", "mcc=3");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig("DDS-Gasoline-pais", "numeroCuenta:pais", "mcc=3");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig("DDS-Restaurant", "numeroCuenta", "mcc=4");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig("DDS-Restaurant-pais", "numeroCuenta:pais", "mcc=4");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig("DDS-Hotel", "numeroCuenta", "mcc=5");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig("DDS-Hotel-pais", "numeroCuenta:pais", "mcc=5");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig("DDS-Hospital", "numeroCuenta", "mcc=6");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig("DDS-Services", "numeroCuenta", "mcc=7");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig("DDS-RentaCar", "numeroCuenta", "mcc=8");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig("DDS-TravelAgency", "numeroCuenta", "mcc=9");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig("DDS-FreeDuty", "numeroCuenta", "mcc=10");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig("DDS-FreeDuty-pais", "numeroCuenta:pais", "mcc=10");
            listaDDS.Add(newConfig);

            //Conectar a Aerospike
            AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);
            
            //Crear politica de escritura para guardar PK en los Bines
            WritePolicy policy = new WritePolicy();
            policy.sendKey = true;
            policy.SetTimeout(50);

            //Estructuras temporales para la creacion de Bines
            List<String> listaNombresBines = new List<String>();
            List<Bin> listaBines = new List<Bin>();

           

            //Datos de entrada
            var reader = new StreamReader(File.OpenRead(@"1000trans.csv"));

            //Variables auxiliares
            bool cabecera = true;
            Key key;
            string trama = "";
            bool primero;
            int contarJson = 0;
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

                    
                    //json antes
                    contarJson++;
                    genJson(client, listaDDS, listaNombresBines, values, contarJson + "a");

                    //Ejecutar dds
                    genDDS(client, policy, listaNombresBines, listaBines, values, trama);
                }
                if(contarJson != 0)
                {
                    //json despues 
                    genJson(client, listaDDS, listaNombresBines, values, contarJson + "d");
                }
                



                key = null;
                listaBines.Clear();
                trama = "";
            }

            TimeSpan timeTaken = timer.Elapsed;
            string resultado = "Tiempo total escritura: " + timeTaken.ToString(@"hh\:mm\:ss\.fff");
            Console.WriteLine(resultado);

            TimeSpan timeLectura = timerLectura.Elapsed;
            string resultado2 = "Tiempo total lectura: " + timeLectura.ToString(@"hh\:mm\:ss\.fff");
            Console.WriteLine(resultado2);
        }

        static Record find(Key key, AerospikeClient client){
            
            Record record = client.Get(null, key);
            if (record != null)
            {
                return record;
            }
            return null;
        }

        static void genDDS(AerospikeClient client, WritePolicy policy, List<String> listaNombresBines, List<Bin> listaBines, string[] values, string trama)
        {
           

            //Variables necesarias
            Key key;

            //Variables de control de acumuladores Dia, Mes, Anio
            bool acumDia = true;
            bool acumMes = true;
            bool acumAnio = true;


            int contador = 0;
            while (contador < listaDDS.Count)
            {
                //incrementar para no enciclarnos va
                contador++;
                //Limpiar lista de Bines temporales                        
                listaBines.Clear();
                //declarar key explicitamente como null para que no se me olvide xd
                key = null;
                
                string stringKey = genStringKey(listaDDS, contador, listaNombresBines, values);

                if (stringKey == null)
                {
                    break;
                }
                //Console.WriteLine(stringKey);
                key = new Key("test", listaDDS[contador - 1].getNombre(), stringKey);//key variable


                //verificar criterio
                bool criterio = true;

                //Paso 1: separar criterios por :
                string cadenaCriterios = listaDDS[contador - 1].getCriterios();

                var criterios = cadenaCriterios.Split(':');

                bool detener = false;
                if (!cadenaCriterios.Equals(""))
                {
                    criterio = true;
                    foreach (var crit in criterios)
                    {
                        //Paso 1.1: por cada criterio, separar bin y valor por =
                        var partesCriterio = crit.Split('=');
                        //Paso 1.2: obtener posicion del bin

                        double pos = obtenerPosicion(listaNombresBines, partesCriterio[0]);
                        if (pos == -1)
                        {
                            detener = true;
                            break;
                        }
                        else
                        {
                            //comparar valor de la entrada con valor de criterio
                            if (!values[(int)pos].Equals(partesCriterio[1]))
                            {
                                criterio = false;
                            }
                        }

                    }
                }




                if (detener == true)
                {
                    break;
                }

                //(int.Parse(values[2].ToString()) == (contador-1)) || ((contador - 1) == 0)||(contador-1 >=4)
                if (criterio == true)
                {

                    //Encontrar bin para decidir si crear bines nuevos o obtener los anteriores
                    bool existe;

                    //funcion para encontrar record
                    timerLectura.Start();
                    Record recordEncontrado = find(key, client);
                    timerLectura.Stop();


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


                        listaBines.Add(new Bin("numeroCuenta", stringKey));

                        listaBines.Add(new Bin("ultimaTrx", trama));
                        if (acumDia == true)
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

                        if (acumMes == true)
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
                        else
                        {
                            listaBines.Add(new Bin("totalAnio", 0));
                            listaBines.Add(new Bin("montoAnio", 0));
                            listaBines.Add(new Bin("higherAnio", 0));
                            listaBines.Add(new Bin("lowerAnio", 0));
                            listaBines.Add(new Bin("mediaAnio", 0));
                        }

                        listaBines.Add(new Bin("fechaTrx", values[6].ToString()));

                        listaBines.Add(new Bin("dateTime1", values[6].ToString()));
                        listaBines.Add(new Bin("dateTime2", ""));
                        listaBines.Add(new Bin("dateTime3", ""));
                        listaBines.Add(new Bin("dateTime4", ""));
                        listaBines.Add(new Bin("dateTime5", ""));

                        // Variables para guardar dia mes y año de la transaccion y reiniciar contadores
                        string fechaTrx = values[6].ToString();
                        var valorsFechaTrx = fechaTrx.Split("-");
                        int diaTrx = int.Parse(valorsFechaTrx[0]);
                        int mesTrx = int.Parse(valorsFechaTrx[1]);
                        int anioTrx = int.Parse(valorsFechaTrx[2]);

                        //PeriodTime Vector 
                        string[] arregloFecha = new string[10];
                        arregloFecha[0] = values[6].ToString();//Guardar fecha formato ddmmYYYY
                        listaBines.Add(new Bin("periodTimeV", arregloFecha));

                        //PeriodTime Count
                        double[] arregloContador = new double[10];
                        arregloContador[0] = 1;//Contador transaccion por dia
                        listaBines.Add(new Bin("periodTimeC", arregloContador));

                        //PeriodTime Amount
                        double[] arregloMonto = new double[10];
                        arregloMonto[0] = double.Parse(values[3]);//Monto del dia
                        listaBines.Add(new Bin("periodTimeA", arregloMonto));
                    }
                    else // Modificar record
                    {


                        Bin temporal;
                        int tempoContar;
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
                        listaBines.Add(new Bin("dateTime1", fechaTrx));
                        listaBines.Add(new Bin("dateTime2", recordEncontrado.GetValue("dateTime1").ToString()));
                        listaBines.Add(new Bin("dateTime3", recordEncontrado.GetValue("dateTime2").ToString()));
                        listaBines.Add(new Bin("dateTime4", recordEncontrado.GetValue("dateTime3").ToString()));
                        listaBines.Add(new Bin("dateTime5", recordEncontrado.GetValue("dateTime4").ToString()));

                        //fecha de trx en DateTime para comparacion
                        DateTime fechaIngreso = DateTime.Parse(fechaTrx);

                        //PeriodTime
                        int contElementos = 0;
                        List<object> listaPeriodVectores = (List<object>)recordEncontrado.GetValue("periodTimeV");
                        string[] arrayPeriodVectores = new string[10];
                        foreach (var elemento in listaPeriodVectores)
                        {
                            if (elemento != null)
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
                        List<object> listaPeriodCount = (List<object>)recordEncontrado.GetValue("periodTimeC");
                        int[] arrayPeriodCount = new int[10];
                        foreach (var elemento in listaPeriodCount)
                        {
                            if (elemento != null)
                            {
                                arrayPeriodCount[contElementos] = Convert.ToInt32(elemento);
                            }
                            else
                            {
                                arrayPeriodCount[contElementos] = 0;
                            }


                            contElementos++;
                        }
                        contElementos = 0;
                        List<object> listaPeriodAmount = (List<object>)recordEncontrado.GetValue("periodTimeA");
                        double[] arrayPeriodAmount = new double[10];
                        foreach (var elemento in listaPeriodAmount)
                        {
                            if (elemento != null)
                            {
                                arrayPeriodAmount[contElementos] = Math.Round((double)elemento, 2);
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

                        for (int i = 0; i < 10; i++)
                        {
                            string elemento = arrayPeriodVectores[i];
                            if (elemento != null)
                            {

                                fechaTemp = DateTime.Parse(elemento.ToString());
                                timeDiff = (fechaIngreso - fechaTemp).TotalDays;
                                if (timeDiff > 0)
                                {
                                    int distancia = c + (int)timeDiff;

                                    if (distancia < 10)
                                    {

                                        arrayPeriodVectores[distancia] = fechaTrx;
                                        arrayPeriodCount[distancia] += 1;
                                        arrayPeriodAmount[distancia] += Math.Round(double.Parse(values[3].ToString()), 2);
                                        break;

                                    }
                                    else//Limpiar y escribir
                                    {

                                        int limpiar = 0;
                                        arrayPeriodVectores[0] = fechaTrx;
                                        arrayPeriodCount[0] = 1;
                                        arrayPeriodAmount[0] = Math.Round(double.Parse(values[3].ToString()), 2);
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
                                        arrayPeriodAmount[c] += Math.Round(double.Parse(values[3].ToString()), 2);
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

                        listaBines.Add(new Bin("periodTimeV", arrayPeriodVectores));
                        listaBines.Add(new Bin("periodTimeC", arrayPeriodCount));
                        listaBines.Add(new Bin("periodTimeA", arrayPeriodAmount));

                        foreach (var bin in recordEncontrado.bins)//Identificar nombre del bin en el record encontrado
                        {
                            switch (bin.Key)
                            {
                                /*Acumulador diario*/

                                case "totalDiario":
                                    if (acumDia == false)
                                    {
                                        break;
                                    }

                                    if (diaTrx > diaInsr || mesTrx > mesInsr || anioTrx > anioInsr)
                                    {
                                        temporal = new Bin("totalDiario", 1);
                                        listaBines.Add(temporal);
                                        break;
                                    }

                                    tempoContar = (int)Math.Round( double.Parse(bin.Value.ToString()),0);
                                    tempoContar++;
                                    temporal = new Bin("totalDiario", tempoContar);
                                    listaBines.Add(temporal);

                                    break;

                                case "montoDiario":
                                    if (acumDia == false)
                                    {
                                        break;
                                    }

                                    if (diaTrx > diaInsr || mesTrx > mesInsr || anioTrx > anioInsr)
                                    {
                                        listaBines.Add(new Bin("montoDiario", Math.Round(double.Parse(values[3]),2)));
                                        break;
                                    }
                                    temporalNumerico1 = double.Parse(bin.Value.ToString());
                                    temporalNumEntrada = double.Parse(values[3].ToString());
                                    temporalNumericoRes = temporalNumerico1 + temporalNumEntrada;

                                    listaBines.Add(new Bin("montoDiario", Math.Round(temporalNumericoRes, 2)));
                                    

                                    break;

                                case "higherDiario":
                                    if (acumDia == false)
                                    {
                                        break;
                                    }

                                    if (diaTrx > diaInsr || mesTrx > mesInsr || anioTrx > anioInsr)
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
                                    if (diaTrx > diaInsr || mesTrx > mesInsr || anioTrx > anioInsr)
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

                                    if (diaTrx > diaInsr || mesTrx > mesInsr || anioTrx > anioInsr)
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
                                    if (acumMes == false)
                                    {
                                        break;
                                    }

                                    if (mesTrx > mesInsr || anioTrx > anioInsr)
                                    {
                                        temporal = new Bin("totalMes", 1);
                                        listaBines.Add(temporal);
                                        break;
                                    }

                                    tempoContar = (int)Math.Round(double.Parse(bin.Value.ToString()), 0);
                                    tempoContar++;
                                    temporal = new Bin("totalMes", tempoContar);
                                    listaBines.Add(temporal);


                                    break;

                                case "montoMes":
                                    if (acumMes == false)
                                    {
                                        break;
                                    }
                                   
                                    if (mesTrx > mesInsr || anioTrx > anioInsr)
                                    {
                                        listaBines.Add(new Bin("montoMes", Math.Round(double.Parse(values[3]), 2)));
                                        break;
                                    }
                                    temporalNumerico1 = double.Parse(bin.Value.ToString());
                                    temporalNumEntrada = double.Parse(values[3].ToString());
                                    temporalNumericoRes = temporalNumerico1 + temporalNumEntrada;

                                    listaBines.Add(new Bin("montoMes", Math.Round(temporalNumericoRes, 2)));
                                    break;

                                case "higherMes":
                                    if (acumMes == false)
                                    {
                                        break;
                                    }
                                    if (mesTrx > mesInsr || anioTrx > anioInsr)
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
                                    if (mesTrx > mesInsr || anioTrx > anioInsr)
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
                                    if (mesTrx > mesInsr || anioTrx > anioInsr)
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

                                    if (acumAnio == false)
                                    {
                                        break;
                                    }
                                    if (anioTrx > anioInsr)
                                    {
                                        temporal = new Bin("totalAnio", 1);
                                        listaBines.Add(temporal);
                                        break;
                                    }

                                    tempoContar = (int)Math.Round(double.Parse(bin.Value.ToString()), 0);
                                    tempoContar++;
                                    temporal = new Bin("totalAnio", tempoContar);
                                    listaBines.Add(temporal);


                                    break;

                                case "montoAnio":
                                    if (acumAnio == false)
                                    {
                                        break;
                                    }
                                    
                                    if (anioTrx > anioInsr)
                                    {
                                        listaBines.Add(new Bin("montoAnio", Math.Round(double.Parse(values[3]), 2)));
                                        break;
                                    }
                                    temporalNumerico1 = double.Parse(bin.Value.ToString());
                                    temporalNumEntrada = double.Parse(values[3].ToString());
                                    temporalNumericoRes = temporalNumerico1 + temporalNumEntrada;

                                    listaBines.Add(new Bin("montoAnio", Math.Round(temporalNumericoRes, 2)));

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
            Console.WriteLine("bin no encontrado: " + busqueda);

            return -1;
        } 

        public static string genStringKey(List<ddsConfig>listaDDS, int contador, List<string> listaNombresBines, string[] values)
        {
            List<double> listaPos = new List<double>();
           
            string argsActuales = listaDDS[contador - 1].getArgs();

            bool detener = false;
            var args = argsActuales.Split(':');

            //Verificar argumentos
            foreach (var arg in args)
            {
                double pos = obtenerPosicion(listaNombresBines, arg);
                if (pos == -1)
                {
                    detener = true;
                    break;
                }
                else
                {
                    listaPos.Add(pos);
                }

            }
            if (detener == true)
            {
                return null;
            }

            string stringKey = "";
            bool primeraPos = true;

            foreach (var pos in listaPos)
            {
                if (primeraPos == true)
                {
                    stringKey = values[(int)pos];
                    primeraPos = false;
                }
                else
                {
                    stringKey += ":" + values[(int)pos];
                }

            }

            return stringKey;
        }        

        public static void genJson(AerospikeClient client, List<ddsConfig> listaDDS,  List<string> listaNombresBines, string[] values, string titulo)
        {
            int contador = 0;
            string salida = "{\"record\": {";
            Record recordEncontrado;
            Key key;
            bool primero = true;


            while (contador < listaDDS.Count)
            {
                contador++;
                string stringKey = genStringKey(listaDDS, contador, listaNombresBines, values);

                if (stringKey == null)
                {
                    break;
                }
                key = new Key("test", listaDDS[contador-1].getNombre(), stringKey);
                recordEncontrado = find(key, client);
        
                if (recordEncontrado != null)
                {
                    if(primero == true)
                    {
                        salida += "\"" + listaDDS[contador - 1].getNombre() + "\":"+JsonConvert.SerializeObject(recordEncontrado);
                        primero = false;
                    }
                    else
                    {
                        salida += ",\"" + listaDDS[contador - 1].getNombre() + "\":" +JsonConvert.SerializeObject(recordEncontrado);
                    }
                   
                }
                
                
            }
            salida += "}}";

            File.WriteAllText(System.AppContext.BaseDirectory + @"/log/" + titulo + @".json", salida);
        }

        
    }
}
