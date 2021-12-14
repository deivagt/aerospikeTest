using Aerospike.Client;
using System;
using System.Diagnostics;
using System.IO;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections;
using Newtonsoft.Json.Converters;
using System.Threading;
using System.Threading.Tasks;

namespace AeroSpike_Test
{
    class Program
    {

        //contadores de escritura/lectura
        static Stopwatch timer = new Stopwatch();
        static Stopwatch timerLectura = new Stopwatch();
        static void Main(string[] args)
        {

            modelado();

            //async client = new AerospikeClient("127.0.0.1", 3000);
            /*Record record = find(new Key("test", "DDS-GLOBAL", "23232323"), client);
            if(record != null)
            {
                Console.WriteLine(JsonConvert.SerializeObject(record));
            }*/
            //23232323

        }

        static void modelado()
        {
            //Propiedades globales
            List<ddsConfig> listaDDS;
            //Iniciar lista de DDS
            listaDDS = new List<ddsConfig>();

            //Declaracion y configuracion de cada DDS a generar
            ddsConfig newConfig;
            newConfig = new ddsConfig("DDS-GLOBAL", "numeroCuenta", "");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig("DDS-POS", "numeroCuenta", "canal=1");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig("DDS-ATM", "numeroCuenta", "canal=2");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig("DDS-ONLINE", "numeroCuenta", "canal=3");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig("DDS-POS-pais", "numeroCuenta:pais", "canal=1");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig("DDS-ATM-pais", "numeroCuenta:pais", "canal=2");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig("DDS-ONLINE-pais", "numeroCuenta:pais", "canal=3");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig("DDS-SuperMarket", "numeroCuenta", "mcc=1");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig("DDS-SuperMarket-pais", "numeroCuenta:pais", "mcc=1");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig("DDS-Pharmacy", "numeroCuenta", "mcc=2");
            listaDDS.Add(newConfig);

            newConfig = new ddsConfig("DDS-Gasoline", "numeroCuenta", "mcc=3");
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


            //Politicas y conexcion a aerospike
            ClientPolicy policy1 = new ClientPolicy();
            policy1.readPolicyDefault.SetTimeout(50);
            policy1.readPolicyDefault.maxRetries = 1;
            policy1.readPolicyDefault.sleepBetweenRetries = 10;
            policy1.writePolicyDefault.SetTimeout(50);
            policy1.writePolicyDefault.maxRetries = 1;
            policy1.writePolicyDefault.sleepBetweenRetries = 50;

            AerospikeClient client = new AerospikeClient( policy1,"127.0.0.1", 3000);

            //Crear politica de escritura para guardar PK en los Bines
            WritePolicy policy = new WritePolicy();
            policy.sendKey = true;
            policy.SetTimeout(50);

            //Estructuras temporales para la creacion de Bines
            List<String> listaNombresBines = new List<String>();
            List<Bin> listaBines = new List<Bin>();


            //Datos de entrada
            var reader = new StreamReader(File.OpenRead(@"trx.csv"));

            //String para guardar la transaccion en un string
            string trama = "";

            //Variables auxiliares
            bool cabecera = true;
            Key key;            
            bool primero;
            int contardorLogJson = 0;
          

            //Lista de hilos activos
            List<Thread> subprocesos;
            List<Task> subtareas;
            while (!reader.EndOfStream)//Hacer cositas con el csv
            {
                //contarDebug++;
                //Iniciar y limpiar lista3
                subprocesos = new List<Thread>();
                subtareas = new List<Task>();
                //Partir csv
                var line = reader.ReadLine();
                var values = line.Split(',');

                //Generar string con la informacion de la transaccion
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
                //Almacenar nombre de bins para utilizar despues
                if (cabecera == true) 
                {
                    foreach (var valor in values)
                    {
                        listaNombresBines.Add(valor);
                    }
                    cabecera = false;
                }
                else
                {
                    //asociar informacion de acuerdo a cada bin leido en la lista de bines
                    //El objetivo es que si el campo es numerico se guarde como un double y si es texto como una cadena
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

                    //Generar el key para guardar la transaccion actual en el set Principal (trxTarjeta)
                    key = new Key("test", "trxTarjeta", values[0]);

                    //Insertar en set trxTarjeta y medir tiempo con timer
                    //No estoy seguro si esta es la manera correcta de medir tiempo de escritura
                    timer.Start();
                    client.Put(policy, key, listaBines.ToArray());
                    timer.Stop();


                    //log json antes de insertar esta trx en dds
                    contardorLogJson++;
                    genJson(client, listaDDS, listaNombresBines, values, contardorLogJson + "antes");

                    //Variable para controlar si la data se genera en modo sincrono/asincrono
                    bool modoAsincriono = true;
                    
                    //Ejecutar creacion de dds en modo sincrono o asincrono
                    foreach (var dds in listaDDS)
                    {
                       // Modo asincrono
                       if(modoAsincriono == true)
                        {
                            //Se inicia el metodo de generacion de DDS y se guarda en una lista para verirficar su estado despues
                            subtareas.Add(Task.Factory.StartNew(() => genDDS(client, policy, listaNombresBines, values, trama, dds)));
                        }
                        else
                        {
                            //Se inicia el metodo de generacion de DDS y se espera a que termine para pasar al siguiente
                            genDDS(client, policy, listaNombresBines, values, trama, dds);
                        }
                    }
                    if(modoAsincriono == true)
                    {
                        Task.WaitAll(subtareas.ToArray());
                    }
                    
                   
                }

                //esta condicion la cree por un error al crear los json que no se porque pasaba jajaja
                if (contardorLogJson != 0)
                {
                    //log json despues de insertar esta trx en dds 
                    genJson(client, listaDDS, listaNombresBines, values, contardorLogJson + "despues");

                }

                //Limpio las variables que se reusan por costumbre
                key = null;
                listaBines.Clear();
                trama = "";
            }

            //Mostrar en consola el tiempo de escritura tomado
            TimeSpan timeTaken = timer.Elapsed;
            string resultado = "Tiempo total escritura: " + timeTaken.ToString(@"hh\:mm\:ss\.fff");
            Console.WriteLine(resultado);

            //Mostrar en consola el tiempo de lectura tomado
            TimeSpan timeLectura = timerLectura.Elapsed;
            string resultado2 = "Tiempo total lectura: " + timeLectura.ToString(@"hh\:mm\:ss\.fff");
            Console.WriteLine(resultado2);
        }

        static Record find(Key key, AerospikeClient client)
        {

            Record record = client.Get(null, key);
            if (record != null)
            {
                return record;
            }
            return null;
        }

        static void genDDS(AerospikeClient client, WritePolicy policy, List<String> listaNombresBines, string[] values, string trama, ddsConfig dds)
        {

            // Limpiar lista de Bines temporales
            /* PARA INSERTAR LOS BINES EN EL RECORD QUE LUEGO SE GUARDA EN AEROSPIKE, CADA UNO SE GUARDA EN LA LISTA "listaBines"
             * QUE LUEGO SE CONVIERTE EN ARRAY Y SE PASA COMO PARAMETRO EN EL PUT
             */
            List<Bin> listaBines = new List<Bin>();


            // Variables necesarias
            Key key;

            // Variables de control de acumuladores Dia, Mes, Anio
            // En el programa principal, el usuario puede elegir si acumular las transacciones por dia, mes y anio
            bool acumDia = true;
            bool acumMes = true;
            bool acumAnio = true;

            


            // declarar key explicitamente como null para que no se me olvide xd
            key = null;

            string stringKey = genStringKey(dds, listaNombresBines, values);

            if (stringKey == null)
            {

                return;
            }
            // Console.WriteLine(stringKey);
            key = new Key("test", dds.getNombre(), stringKey);//key variable


            // verificar criterio
            bool criterio = true;

            // Paso 1: separar criterios por :
            string cadenaCriterios = dds.getCriterios();

            var criterios = cadenaCriterios.Split(':');

            // Variable para detener el metodo si alguna condicion principal no se cumple
            bool detener = false;

            // Verificar si lops campos de la trx tiene valores que hagan match con los criterios de cada DDS
            if (!cadenaCriterios.Equals(""))
            {
                criterio = true;
                foreach (var crit in criterios)
                {
                    // Paso 1  por cada criterio, separar bin y valor por =
                    var partesCriterio = crit.Split('=');

                    // Paso 2: obtener posicion del bin

                    double pos = obtenerPosicion(listaNombresBines, partesCriterio[0]);
                    if (pos == -1)
                    {
                        detener = true;

                        break;
                    }
                    else
                    {
                        // comparar valor de la entrada con valor de criterio
                        if (!values[(int)pos].Equals(partesCriterio[1]))
                        {
                            criterio = false;
                        }
                    }

                }
            }

            if (detener == true)
            {
                return;
            }

           
            if (criterio == true)
            {

                // Encontrar bin para decidir si crear bines nuevos o obtener los anteriores
                bool existe;

                // funcion para encontrar record
                // No estoy seguro si esta es la manera correcta de tomar el tiempo de los bines
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

                // Generar bines predeterminados
                if (existe == false)
                {

                    /* EN EL CASO DE ESTE PROGRAMA SE CONOCE EN QUE FORMA LLEGAN LOS DATOS, POR TANTO LA POSICION DE LA TRX 
                     EN LA QUE SE OBTIENE LOS DATOS ESTA QUEMADA, ES NECESARIO VOLVER ESTO DINAMICO*/

                    // Se asume el siguiente orden de los campos:
                    //  [0]idTrans,[1]mcc, [2]canal, [3]monto, [4]bin, [5]numeroCuenta, [6]date, [7]pais

                    //Generar bines
                    listaBines.Add(new Bin("numeroCuenta", stringKey));
                    listaBines.Add(new Bin("ultimaTrx", trama));

                    string fechaTrx = values[6].ToString();
                    listaBines.Add(new Bin("fechaTrx", fechaTrx));

                    //Crear bines para acumulardor de trx diario
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
                    //Crear bines para acumulardor de trx mensual
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

                    //Crear bines para acumulardor de trx anual
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

                    
                    //Campos usados en dds
                    listaBines.Add(new Bin("dateTime1", values[6].ToString()));
                    listaBines.Add(new Bin("dateTime2", ""));
                    listaBines.Add(new Bin("dateTime3", ""));
                    listaBines.Add(new Bin("dateTime4", ""));
                    listaBines.Add(new Bin("dateTime5", ""));
                    
                    //mas informacion que entra en la dds
                    // En esta parte, en cada bin "periodxxxx" la informacion guardada en cada bin es un array ya que aerospike lo permite
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
                    /* EN EL CASO DE ESTE PROGRAMA SE CONOCE EN QUE FORMA LLEGAN LOS DATOS, POR TANTO LA POSICION DE LA TRX 
                     EN LA QUE SE OBTIENE LOS DATOS ESTA QUEMADA, ES NECESARIO VOLVER ESTO DINAMICO*/

                    // Se asume el siguiente orden de los campos:
                    // [0]idTrans,[1]mcc, [2]canal, [3]monto, [4]bin, [5]numeroCuenta, [6]date, [7]pais


                    //Mucho del codigo en esta parte es para crear nueva informacion que luego se guarda en aerospike

                    //Falta medir mucho tiempo de lectura


                    //Muchas variables usadas para generar informacion que luego se introduce a aerospike
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

                    //fecha guardada en el record
                    string fechaInsr = recordEncontrado.GetValue("fechaTrx").ToString();
                    var valoresFechaInsr = fechaInsr.Split('-');
                    int diaInsr = int.Parse(valoresFechaInsr[0]);
                    int mesInsr = int.Parse(valoresFechaInsr[1]);
                    int anioInsr = int.Parse(valoresFechaInsr[2]);

                    //Actualizar ultima trx, siempre se realiza
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

                    // En esta parte, en cada bin "periodxxxx" la informacion guardada en cada bin es un array ya que aerospike lo permite

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
                        //Holi, soy un incremento de variable, si me quitas te enciclo el programa c:
                        c++;
                    }
                    //Agregar los bines "periodxxx"
                    listaBines.Add(new Bin("periodTimeV", arrayPeriodVectores));
                    listaBines.Add(new Bin("periodTimeC", arrayPeriodCount));
                    listaBines.Add(new Bin("periodTimeA", arrayPeriodAmount));


                    //Esta parte genera los acumuladores de dia, mes y anio
                    //Fue hecho como ciclo porque anteriormente el metodo generaba todas las DDS con un while
                    //Probablemente el foreach pueda ser retirado

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

                                tempoContar = (int)Math.Round(double.Parse(bin.Value.ToString()), 0);
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
                                    listaBines.Add(new Bin("montoDiario", Math.Round(double.Parse(values[3]), 2)));
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

                //HACER INSERCION DEL KEY EN SET
                //No estoy seguro si esta es la manera correcta de medir el tiempo de escritura
                timer.Start();
                client.Put(policy, key, listaBines.ToArray());
                timer.Stop();


            }



        }

        static double obtenerPosicion(List<String> listaNombresBines, string busqueda)
        {
            int i = 0;
            foreach (var nombreBin in listaNombresBines)
            {
                if (nombreBin == busqueda)
                {
                    return i;
                }
                i++;
            }
            Console.WriteLine("bin no encontrado: " + busqueda);

            return -1;
        }

        /*El metodo genStringKey se encarga de generar un string el cual sirve de PK para diferenciar cada record segun criterios de la dds
         * por ejemplo, para la DDS GLOBAL el pk es solamente el numero de cuenta, -> "56565656"
         * para la DDS ATM PAIS, elk pk es el numero de cuenta, canal de trx (atm) y el pais -> "56565656:2:belice"
         * 
         * En este ejemplo, el  numero de cuenta es "56565656", el tipo de canal es "2" y el pais "belice"
         * 
         * Cada criterio se separa por ":"
         * 
         * Estructura:
         * 
         *  critrerio : criterio : criterio: ...
         */
        public static string genStringKey(ddsConfig dds, List<string> listaNombresBines, string[] values)
        {
            List<double> listaPos = new List<double>();

            string argsActuales = dds.getArgs();

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

        /*
         * Este metodo genera un archivo json buscando la informacion de una trx en cada dds y estructurandola en un archivo json
         */
        public static void genJson(AerospikeClient client, List<ddsConfig> listaDDS, List<string> listaNombresBines, string[] values, string titulo)
        {
            int contador = 0;
            string salida = "{\"record\": {";
            Record recordEncontrado;
            Key key;
            bool primero = true;


            while (contador < listaDDS.Count)
            {
                contador++;
                string stringKey = genStringKey(listaDDS[contador - 1], listaNombresBines, values);

                if (stringKey == null)
                {
                    break;
                }
                key = new Key("test", listaDDS[contador - 1].getNombre(), stringKey);
                recordEncontrado = find(key, client);

                if (recordEncontrado != null)
                {
                    if (primero == true)
                    {
                        salida += "\"" + listaDDS[contador - 1].getNombre() + "\":" + JsonConvert.SerializeObject(recordEncontrado);
                        primero = false;
                    }
                    else
                    {
                        salida += ",\"" + listaDDS[contador - 1].getNombre() + "\":" + JsonConvert.SerializeObject(recordEncontrado);
                    }

                }


            }
            salida += "}}";

            File.WriteAllText(System.AppContext.BaseDirectory + @"/log/" + titulo + @".json", salida);
        }


    }
}
