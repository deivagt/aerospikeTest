

using Aerospike.Client;
using System;
using System.Diagnostics;
using System.IO;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace AeroSpike_Test
{
    class Test
    {
        //Esta clase fue usada para empezar a entender aerospike, el codigo principal existe en la clase Program.cs
        static void ejemploModificacion()
        {
            AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);
            WritePolicy policy = new WritePolicy();
            policy.sendKey = true;
            Key key = new Key("test", "pruebaModificacion", "primero");
            Bin bin1 = new Bin("nombre", "David");
            client.Put(policy, key, bin1);

            Record read = client.Get(policy, key);
            string nombre = read.GetValue("nombre").ToString();
            Console.WriteLine(nombre);

            bin1 = new Bin("nombre", "Luis");
            Bin bin2 = new Bin("edad", 20);
            client.Put(policy, key, bin1, bin2);

            read = client.Get(policy, key);
            nombre = read.GetValue("nombre").ToString();
            Console.WriteLine(nombre);
        }
        /**************/
        static void transacciones()
        {
            //Conectar a Aerospike
            AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);
            //client.DropIndex(null, "test", "llave", "indexPorTarjeta");

            //Crear index secundario para acelerar las busquedas MULTIPLES
            //crearIndex(client, "test", "llave", "indexPorTarjeta", "tarjeta");

            //Crear politica de escritura para guardar PK en los Bines
            WritePolicy policy = new WritePolicy();
            policy.sendKey = true;

            //Estructuras temporales para la creacion de Bines
            List<String> listaNombresBines = new List<String>();
            List<Bin> listaBines = new List<Bin>();

            //contadores de escritura/lectura
            var timer = new Stopwatch();
            var timerLectura = new Stopwatch();

            //Datos de entrada
            var reader = new StreamReader(File.OpenRead(@"trx1.csv"));

            //Variables auxiliares
            Bin contador;
            Record record;
            bool cabecera = true;
            Key key;

            while (!reader.EndOfStream)//Hacer cositas con el csv
            {
                //Partir csv
                var line = reader.ReadLine();
                var values = line.Split(',');
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
                    double i = 0;
                    int contar = 0;
                    foreach (var nombre in listaNombresBines)
                    {
                        if (double.TryParse(values[contar], out i))
                        {
                            listaBines.Add(new Bin(nombre, i));
                        }
                        else
                        {
                            listaBines.Add(new Bin(nombre, values[contar]));
                        }
                        contar++;

                    }

                    key = new Key("test", "transacciones", values[0] + ':' + values[3] + ':' + values[4]);

                    //Insertar en SET TRANSACCIONES
                    timer.Start();
                    client.Put(policy, key, listaBines.ToArray());
                    timer.Stop();

                    //Limpiar key y Lista de Bines temporales
                    key = null;
                    listaBines.Clear();

                    //Guardar dds TARJETA + FECHA NO DINAMICO --------------------------------------------------

                    key = new Key("test", "llave", values[0] + ':' + values[3]);//tarjeta y fecha
                    timerLectura.Start();
                    record = client.Get(null, key);
                    timerLectura.Stop();
                    if (record != null)
                    {
                        contador = null;
                        //Update
                        foreach (var bin in record.bins)
                        {

                            if (bin.Key == "contador")
                            {
                                int update = Int32.Parse(bin.Value.ToString()) + 1;
                                contador = new Bin("contador", update);
                            }

                        }
                        timer.Start();
                        client.Put(policy, key, contador);
                        timer.Stop();
                    }
                    else
                    {//PARTE NO DINAMICA

                        listaBines.Add(new Bin("tipo", "fecha"));//Agregar tipo
                        //Create
                        contar = 0;
                        foreach (var nombre in listaNombresBines)
                        {
                            if (nombre == "tarjeta" || nombre == "fecha")
                            {
                                if (double.TryParse(values[contar], out i))
                                {
                                    listaBines.Add(new Bin(nombre, i));
                                }
                                else
                                {
                                    listaBines.Add(new Bin(nombre, values[contar]));
                                }
                            }
                            contar++;
                        }
                        listaBines.Add(new Bin("contador", 1));//Agregar contador

                        //Insertar en SET LLAVE -Criterio Tarjeta:Fecha 
                        timer.Start();
                        client.Put(policy, key, listaBines.ToArray());
                        timer.Stop();

                    }



                    //Guardar dds TARJETA + FECHA+ PAIS NO DINAMICO --------------------------------------------------

                    key = new Key("test", "llave", values[0] + ':' + values[3] + ':' + values[6]);//tarjeta, fecha y pais
                    timerLectura.Start();
                    record = client.Get(policy, key);
                    timerLectura.Stop();
                    if (record != null)
                    {
                        contador = null;
                        //Update
                        foreach (var bin in record.bins)
                        {

                            if (bin.Key == "contador")
                            {
                                int update = Int32.Parse(bin.Value.ToString()) + 1;
                                contador = new Bin("contador", update);
                            }

                        }
                        timer.Start();
                        client.Put(null, key, contador);
                        timer.Stop();
                    }
                    else
                    {//PARTE NO DINAMICA

                        listaBines.Add(new Bin("tipo", "pais"));//Agregar tipo
                        //Create
                        contar = 0;
                        foreach (var nombre in listaNombresBines)
                        {

                            if (nombre == "tarjeta" || nombre == "fecha" || nombre == "pais")
                            {
                                if (double.TryParse(values[contar], out i))
                                {
                                    listaBines.Add(new Bin(nombre, i));
                                }
                                else
                                {
                                    listaBines.Add(new Bin(nombre, values[contar]));
                                }
                            }
                            contar++;
                        }
                        listaBines.Add(new Bin("contador", 1));//Agregar contador

                        //Insertar en SET LLAVE -Criterio Tarjeta:Fecha:Pais
                        timer.Start();
                        client.Put(policy, key, listaBines.ToArray());
                        timer.Stop();

                    }

                }

                listaBines.Clear();



            }

            TimeSpan timeTaken = timer.Elapsed;
            string resultado = "Tiempo total: " + timeTaken.ToString(@"hh\:mm\:ss\.fff");
            Console.WriteLine(resultado);


            TimeSpan timeTakenLectura = timerLectura.Elapsed;
            string resultadoLectura = "Tiempo total: " + timeTakenLectura.ToString(@"hh\:mm\:ss\.fff");
            Console.WriteLine(resultadoLectura);




        }

        static void crearIndex(AerospikeClient client, string ns, string set, string indexName, string binName)
        {
            Console.WriteLine("Create index");
            Policy policy = new Policy();
            policy.totalTimeout = 0; // Do not timeout on index create.
            IndexTask task = client.CreateIndex(policy, ns, set, indexName, binName, IndexType.NUMERIC);
            task.Wait();
        }


        static void RunQuery(AerospikeClient client, string ns, string set, string indexName, string binName)
        {
            Console.WriteLine("Query");
            Statement stmt = new Statement();
            stmt.SetNamespace(ns);
            stmt.SetSetName(set);
            stmt.SetIndexName(indexName);
            stmt.SetFilter(Filter.Equal("tarjeta", 4080000000000000));

            RecordSet rs = client.Query(null, stmt);

            try
            {
                while (rs.Next())
                {
                    Key key = rs.Key;
                    Record record = rs.Record;
                    object result = record.GetValue(binName);
                    Console.WriteLine("Record found: ns=" + key.ns +
                        " set=" + key.setName +
                        " bin=" + binName +
                        " digest=" + ByteUtil.BytesToHexString(key.digest) +
                        " value=" + result);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("No encontrado");
            }
            finally
            {
                rs.Close();
            }
        }
        static void create()
        {
            // Establish connection the server
            AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);

            // Create key
            //Key key = new Key("test", "pruebaUsers2", "mykey");

            // Create Bins
            Bin bin1;
            Bin bin2;
            Bin bin3;
            Bin bin4;

            //timer
            var timer = new Stopwatch();

            //Datos de entrada
            var reader = new StreamReader(File.OpenRead(@"source.csv"));
            usuario temp;
            List<usuario> miLista = new List<usuario>();

            while (!reader.EndOfStream)
            {
                var line = reader.ReadLine();
                var values = line.Split(',');
                temp = new usuario(Int32.Parse(values[0]), values[1], values[2], values[3]);
                miLista.Add(temp);
            }

            //Prueba de insercion
            foreach (var e in miLista)
            {


                Key key = new Key("test", "usuarios", e.id);
                bin1 = new Bin("id", e.id);
                bin2 = new Bin("nombre", e.nombre);
                bin3 = new Bin("apellido", e.apellido);
                bin4 = new Bin("ciudad", e.ciudad);
                timer.Start();
                client.Put(null, key, bin1, bin2, bin3, bin4);
                key = new Key("test", "usuarios", e.id + 1);
                client.Put(null, key, bin4);
                timer.Stop();
            }


            //Procesado de resultado
            TimeSpan timeTaken = timer.Elapsed;
            string resultado = "Tiempo total: " + timeTaken.ToString(@"m\:ss\.fff");
            Console.WriteLine(resultado);

            //Read record
            /*Record record = client.Get(null, new Key("test", "usuarios", 103634));
            foreach (var xd in record.bins)
            {
                Console.WriteLine(xd);
            }*/

            // Close connection
            client.Close();
        }

        static void update()
        {
            // Establish connection the server
            AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);

            // Create key
            //Key key = new Key("test", "pruebaUsers2", "mykey");

            // Create Bins
            Bin bin1;
            Bin bin2;
            Bin bin3;
            Bin bin4;

            //timer
            var timer = new Stopwatch();

            //Datos de entrada
            var reader = new StreamReader(File.OpenRead(@"update.csv"));
            usuario temp;
            List<usuario> miLista = new List<usuario>();

            while (!reader.EndOfStream)
            {
                var line = reader.ReadLine();
                var values = line.Split(',');
                temp = new usuario(Int32.Parse(values[0]), values[1], values[2], values[3]);
                miLista.Add(temp);
            }

            //Prueba de insercion
            foreach (var e in miLista)
            {
                Key key = new Key("test", "usuarios", e.id);
                bin1 = new Bin("id", e.id);
                bin2 = new Bin("nombre", e.nombre);
                bin3 = new Bin("apellido", "Perez");
                bin4 = new Bin("ciudad", e.ciudad);
                timer.Start();
                client.Put(null, key, bin1, bin2, bin3, bin4);
                timer.Stop();
            }

            //Read record
            Record record = client.Get(null, new Key("test", "usuarios", 200000));
            foreach (var xd in record.bins)
            {
                Console.WriteLine(xd);
            }
            //Procesado de resultado
            TimeSpan timeTaken = timer.Elapsed;
            string resultado = "Tiempo total: " + timeTaken.ToString(@"m\:ss\.fff");
            Console.WriteLine(resultado);

            // Close connection
            client.Close();
        }
        static void onePush()
        {
            AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);

            // Create key
            //Key key = new Key("test", "pruebaUsers2", "mykey");

            // Create Bins
            Bin bin1;
            Bin bin2;
            Bin bin3;
            Bin bin4;
            Bin bin5;

            //timer
            var timer = new Stopwatch();

            Key key = new Key("test", "a", "5216516516:21202223");


            bin1 = new Bin("id", "519198");
            bin2 = new Bin("nombre", "David");
            bin3 = new Bin("apellido", "Belisle");
            bin4 = new Bin("ciudad", "Guatemala");
            bin5 = new Bin("Edad", 21);
            timer.Start();

            client.Put(null, key, bin1, bin2, bin3, bin4, bin5);
            timer.Stop();

            key = new Key("test", "a", "5216516516:21202224");
            client.Put(null, key, bin5);
            key = new Key("test", "a", "5216516516:21202223");
            Record record = client.Get(null, key);
            if (record != null)
            {
                foreach (var bin in record.bins)
                {
                    Console.WriteLine(bin);
                    if (bin.Key == "Edad")
                    {
                        int update = Int32.Parse(bin.Value.ToString()) + 1;
                        bin5 = new Bin("Edad", update);

                    }
                }
            }

            //client.Put(null, key ,bin5);
            key = new Key("test", "a", "5216516516:21202224");
            record = client.Get(null, key);
            if (record != null)
            {
                foreach (var bin in record.bins)
                {
                    Console.WriteLine(bin);
                }
            }




            TimeSpan timeTaken = timer.Elapsed;
            string resultado = "Tiempo total: " + timeTaken.ToString(@"m\:ss\.fff");
            Console.WriteLine(resultado);
        }

        static void insercionAleatoria()
        {
            Random rnd = new Random();


            AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);

            // Create key
            //Key key = new Key("test", "pruebaUsers2", "mykey");

            // Create Bins
            Bin bin1;
            Bin bin2;
            Bin bin3;
            Bin bin4;
            //timer
            var timer = new Stopwatch();
            int contador = 0;

            while (contador <= 100)
            {
                int keyNmbr = rnd.Next(1, 500);
                Key key = new Key("test", "generatedKey", keyNmbr);
                bin1 = new Bin("id", keyNmbr);
                bin2 = new Bin("nombre", "David");
                bin3 = new Bin("apellido", "Belisle");
                bin4 = new Bin("ciudad", "Guatemala");
                /*if(!find(keyNmbr, client)){
                    timer.Start();
                    client.Put(null, key, bin1, bin2, bin3, bin4);
                    timer.Stop();
                }*/
                contador++;
            }


            TimeSpan timeTaken = timer.Elapsed;
            string resultado = "Tiempo total: " + timeTaken.ToString(@"m\:ss\.fff");
            Console.WriteLine(resultado);
        }



        static void read()
        {
            AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);
            List<Key> keys = new List<Key>();

            for (int i = 1; i < 100; i++)
            {
                keys.Add(new Key("test", "generatedKey", (i + 1)));
            }

            Record[] records = client.Get(null, keys.ToArray());
            Console.WriteLine("ID | \nNOMBRE | \nAPELLIDO | \nCIUDAD |");


            foreach (var record in records)
            {
                if (record != null)
                {
                    Console.WriteLine("-------");
                    foreach (var bin in record.bins)
                    {
                        Console.WriteLine(bin);
                    }
                }


            }
        }

        static void query()
        {
            AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);

            Console.WriteLine("Query");
            Statement stmt = new Statement();
            stmt.SetNamespace("test");
            stmt.SetSetName("generatedKey");
            stmt.SetBinNames("id");
            stmt.SetFilter(Filter.Equal("id", 2));

            RecordSet rs = client.Query(null, stmt);


            try
            {
                while (rs.Next())
                {
                    Key key = rs.Key;
                    Record record = rs.Record;
                    object result = record.GetValue("id");
                    Console.WriteLine("Record found: ns=" + key.ns +
                        " set=" + key.setName +
                        " bin=" + "id" +
                        " digest=" + ByteUtil.BytesToHexString(key.digest) +
                        " value=" + result);
                }
            }
            finally
            {
                rs.Close();
            }

        }

    }
}
