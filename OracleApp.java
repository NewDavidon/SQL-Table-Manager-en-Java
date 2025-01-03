import java.sql.*;
import java.util.Scanner;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

public class OracleApp {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        Connection connection = null;

        while (true) {
            connection = conectarBaseDeDatos(scanner);
            if (connection != null) {
                break; // Salir del bucle si la conexión es exitosa
            }
            System.out.println("¿Desea intentar conectarse nuevamente? (s/n): ");
            String respuesta = scanner.nextLine().trim().toLowerCase();
            if (!respuesta.equals("s")) {
                System.out.println("Saliendo del programa...");
                return;
            }
        }

        mostrarMenu(connection, scanner);

        cerrarConexion(connection);
        scanner.close();
    }

    private static Connection conectarBaseDeDatos(Scanner scanner) {
        System.out.print("Ingrese usuario de la base de datos: ");
        String dbUser = scanner.nextLine();
        System.out.print("Ingrese contraseña: ");
        char[] dbPassword = System.console() != null 
            ? System.console().readPassword() : scanner.nextLine().toCharArray();

        try {
            Class.forName("oracle.jdbc.OracleDriver");
            return DriverManager.getConnection(
                "jdbc:oracle:thin:@localhost:1521:XE", dbUser, new String(dbPassword));
        } catch (ClassNotFoundException e) {
            System.out.println("Error al cargar el controlador JDBC.");
        } catch (SQLException e) {
            System.out.println("Usuario y contraseña incorrectos. " + e.getMessage());
        }
        return null;
    }

    private static void mostrarMenu(Connection connection, Scanner scanner) {
        int opcion;
        do {
            System.out.println("\nMenú:");
            System.out.println("1. Mantenimiento de datos de una tabla");
            System.out.println("2. Consulta total de datos de cualquier tabla");
            System.out.println("3. Salir");
            System.out.print("Seleccione una opción: ");

            while (!scanner.hasNextInt()) {
                System.out.println("Entrada inválida. Por favor ingrese un número.");
                scanner.next();
            }

            opcion = scanner.nextInt();
            scanner.nextLine();

            switch (opcion) {
                case 1:
                    mantenimientoDeDatos(connection, scanner);
                    break;
                case 2:
                    consultarTodosLosDatos(connection, scanner);
                    break;
                case 3:
                    System.out.println("Saliendo del programa...");
                    break;
                default:
                    System.out.println("Opción inválida, intente de nuevo.");
            }
        } while (opcion != 3);
    }

    private static void mantenimientoDeDatos(Connection connection, Scanner scanner) {
        int opcion;
        System.out.print("Ingrese el nombre de la tabla: ");
        String nombreTabla = scanner.nextLine();

        if (!tablaExiste(connection, nombreTabla)) {
            System.out.println("Tabla inexistente.");
            return;
        }

        do {
            System.out.println("\nMantenimiento de datos de la tabla " + nombreTabla + ":");
            System.out.println("1. Añadir una fila");
            System.out.println("2. Visualizar datos de una fila");
            System.out.println("3. Consultar filas con condición");
            System.out.println("4. Modificar una columna");
            System.out.println("5. Eliminar una fila");
            System.out.println("6. Volver al menú principal");
            System.out.print("Seleccione una opción: ");

            while (!scanner.hasNextInt()) {
                System.out.println("Entrada inválida. Por favor ingrese un número.");
                scanner.next();
            }

            opcion = scanner.nextInt();
            scanner.nextLine(); // Limpiar buffer

            switch (opcion) {
                case 1:
                    añadirFila(connection, scanner, nombreTabla);
                    break;
                case 2:
                    visualizarFila(connection, scanner, nombreTabla);
                    break;
                case 3:
                    consultarConCondicion(connection, scanner, nombreTabla);
                    break;
                case 4:
                    modificarColumna(connection, scanner, nombreTabla);
                    break;
                case 5:
                    eliminarFila(connection, scanner, nombreTabla);
                    break;
                case 6:
                    System.out.println("Volviendo al menú principal...");
                    break;
                default:
                    System.out.println("Opción inválida, intente de nuevo.");
            }
        } while (opcion != 6);
    }

    private static void consultarTodosLosDatos(Connection connection, Scanner scanner) {
        System.out.print("Ingrese el nombre de la tabla: ");
        String nombreTabla = scanner.nextLine();

        if (!tablaExiste(connection, nombreTabla)) {
            System.out.println("Tabla inexistente.");
            return;
        }

        String query = "SELECT * FROM " + nombreTabla;
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(query)) {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            System.out.println("\nDatos de la tabla " + nombreTabla + ":");
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(metaData.getColumnName(i) + "\t");
            }
            System.out.println();

            int rowCount = 0;
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(rs.getString(i) + "\t");
                }
                System.out.println();
                rowCount++;
            }
            System.out.println(rowCount + " filas consultadas.");
        } catch (SQLException e) {
            System.out.println("Error al consultar los datos: " + e.getMessage());
        }
    }

    private static boolean tablaExiste(Connection connection, String nombreTabla) {
        String query = "SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = ?";
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setString(1, nombreTabla.toUpperCase());
            ResultSet rs = preparedStatement.executeQuery();
            return rs.next() && rs.getInt(1) > 0;
        } catch (SQLException e) {
            System.out.println("Error al verificar la existencia de la tabla: " + e.getMessage());
            return false;
        }
    }

    private static void añadirFila(Connection connection, Scanner scanner, String nombreTabla) {
        String primaryKey = obtenerClavePrimaria(connection, nombreTabla);
        if (primaryKey == null) {
            System.out.println("No se puede añadir una fila porque no se encontró la clave primaria.");
            return;
        }

        System.out.println("Añadiendo una nueva fila a la tabla " + nombreTabla + "...");

        try {
            // Obtener las columnas de la tabla (excepto la clave primaria si es autogenerada).
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet rsColumns = metaData.getColumns(null, null, nombreTabla.toUpperCase(), null);

            // Almacenar las columnas y tipos en listas.
            List<String> columnas = new ArrayList<>();
            List<String> tipos = new ArrayList<>();

            while (rsColumns.next()) {
                String columnName = rsColumns.getString("COLUMN_NAME");
                String columnType = rsColumns.getString("TYPE_NAME");

                // Excluir la clave primaria si es autogenerada.
                if (!columnName.equalsIgnoreCase(primaryKey)) {
                    columnas.add(columnName);
                    tipos.add(columnType);
                }
            }
            rsColumns.close();

            if (columnas.isEmpty()) {
                System.out.println("No hay columnas disponibles para insertar.");
                return;
            }

            // Solicitar valores al usuario para cada columna.
            Map<String, Object> valores = new HashMap<>();
            for (int i = 0; i < columnas.size(); i++) {
                System.out.print("Ingrese el valor para " + columnas.get(i) + " (" + tipos.get(i) + "): ");
                String input = scanner.nextLine();

                // Convertir dinámicamente los valores según el tipo de dato de la columna.
                Object valorConvertido;
                switch (tipos.get(i)) {
                    case "VARCHAR2":
                        valorConvertido = input; // Se guarda como String.
                        break;
                    case "NUMBER":
                        valorConvertido = Integer.parseInt(input); // Convertir a Integer.
                        break;
                    case "DATE":
                        valorConvertido = java.sql.Date.valueOf(input); // Convertir a Date (formato: yyyy-MM-dd).
                        break;
                    default:
                        valorConvertido = input; // Por defecto, tratar como String.
                }
                valores.put(columnas.get(i), valorConvertido);
            }

            // Construir dinámicamente la consulta de inserción.
            StringBuilder columnasQuery = new StringBuilder(primaryKey + ", ");// Inicia con la clave primaria.
            StringBuilder valoresQuery = new StringBuilder("?, ");// Inicia con el marcador de posición para la clave primaria.
            
            // Recorrer todas las columnas adicionales (excluyendo la clave primaria).
            for (String columna : columnas) {
                columnasQuery.append(columna).append(", ");
                valoresQuery.append("?, ");
            }

            // Eliminar las últimas comas y espacios.
            columnasQuery.setLength(columnasQuery.length() - 2);
            valoresQuery.setLength(valoresQuery.length() - 2);

            // Obtener el próximo ID para la clave primaria.
            String queryID = "SELECT NVL(MAX(" + primaryKey + "), 0) + 1 AS NEXT_ID FROM " + nombreTabla;
            int nuevoID = 1;
            try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(queryID)) {
                if (rs.next()) {
                    nuevoID = rs.getInt("NEXT_ID");
                }
            }

            // Construir y ejecutar la consulta de inserción.
            String insertQuery = "INSERT INTO " + nombreTabla + " (" + columnasQuery + ") VALUES (" + valoresQuery + ")";
            try (PreparedStatement pstmt = connection.prepareStatement(insertQuery)) {
                pstmt.setInt(1, nuevoID); // Establecer el valor para la clave primaria.

                // Establecer los valores para las columnas dinámicamente.
                int index = 2;
                for (String columna : columnas) {
                    Object valor = valores.get(columna);
                    if (valor instanceof String) {
                        pstmt.setString(index, (String) valor);
                    } else if (valor instanceof Integer) {
                        pstmt.setInt(index, (Integer) valor);
                    } else if (valor instanceof java.sql.Date) {
                        pstmt.setDate(index, (java.sql.Date) valor);
                    } else {
                        pstmt.setObject(index, valor); // Caso genérico.
                    }
                    index++;
                }

                pstmt.executeUpdate();
                System.out.println("Fila añadida exitosamente con " + primaryKey + ": " + nuevoID);
            }
        } catch (SQLException e) {
            System.out.println("Error al añadir una fila: " + e.getMessage());
        } catch (NumberFormatException e) {
            System.out.println("Error al convertir un valor numérico. Verifique sus entradas: " + e.getMessage());
        } catch (IllegalArgumentException e) {
            System.out.println("Error al convertir una fecha. Use el formato yyyy-MM-dd: " + e.getMessage());
        }
    }



    private static void visualizarFila(Connection connection, Scanner scanner, String nombreTabla) {
        String primaryKey = obtenerClavePrimaria(connection, nombreTabla);
        if (primaryKey == null) {
            System.out.println("No se puede consultar una fila porque no se encontró la clave primaria.");
            return;
        }

        System.out.print("Ingrese el valor de la clave primaria (" + primaryKey + "): ");
        String valorClavePrimaria = scanner.nextLine();

        String query = "SELECT * FROM " + nombreTabla + " WHERE " + primaryKey + " = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(query)) {
            pstmt.setString(1, valorClavePrimaria);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();

                    System.out.println("\nDatos de la fila:");
                    for (int i = 1; i <= columnCount; i++) {
                        System.out.println(metaData.getColumnName(i) + ": " + rs.getString(i));
                    }
                } else {
                    System.out.println("No se encontró una fila con " + primaryKey + ": " + valorClavePrimaria);
                }
            }
        } catch (SQLException e) {
            System.out.println("Error al consultar la fila: " + e.getMessage());
        }
    }

    private static void consultarConCondicion(Connection connection, Scanner scanner, String nombreTabla) {
        System.out.print("Ingrese el nombre de la columna para la condición: ");
        String columna = scanner.nextLine();
        System.out.print("Ingrese el valor para filtrar: ");
        String valor = scanner.nextLine();

        String query = "SELECT * FROM " + nombreTabla + " WHERE " + columna + " = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(query)) {
            pstmt.setString(1, valor);
            try (ResultSet rs = pstmt.executeQuery()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                System.out.println("\nFilas que cumplen la condición:");
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(metaData.getColumnName(i) + "\t");
                }
                System.out.println();

                int rowCount = 0;
                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        System.out.print(rs.getString(i) + "\t");
                    }
                    System.out.println();
                    rowCount++;
                }
                System.out.println(rowCount + " filas encontradas.");
            }
        } catch (SQLException e) {
            System.out.println("Error al consultar las filas: " + e.getMessage());
        }
    }
    private static void modificarColumna(Connection connection, Scanner scanner, String nombreTabla) {
        System.out.print("Ingrese el ID de la fila a modificar: ");
        int id = scanner.nextInt();
        scanner.nextLine(); // Limpiar buffer

        System.out.print("Ingrese el nombre de la columna a modificar: ");
        String columna = scanner.nextLine();
        System.out.print("Ingrese el nuevo valor para la columna: ");
        String nuevoValor = scanner.nextLine();

        // Construir la consulta SQL para actualizar la columna.
        String updateQuery = "UPDATE " + nombreTabla + " SET " + columna + " = ? WHERE ID = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(updateQuery)) {
            pstmt.setString(1, nuevoValor); // Establecer el nuevo valor.
            pstmt.setInt(2, id); // Establecer el ID de la fila.

            int rowsUpdated = pstmt.executeUpdate(); // Ejecutar la actualización.
            if (rowsUpdated > 0) {
                System.out.println("Fila actualizada exitosamente.");
            } else {
                System.out.println("No se encontró una fila con ID: " + id);
            }
        } catch (SQLException e) {
            System.out.println("Error al modificar la fila: " + e.getMessage());
        }
    }
    private static void eliminarFila(Connection connection, Scanner scanner, String nombreTabla) {
        String primaryKey = obtenerClavePrimaria(connection, nombreTabla);
        if (primaryKey == null) {
            System.out.println("No se puede eliminar una fila porque no se encontró la clave primaria.");
            return;
        }

        System.out.print("Ingrese el valor de la clave primaria (" + primaryKey + "): ");
        String valorClavePrimaria = scanner.nextLine();

        System.out.print("¿Está seguro de que desea eliminar esta fila? (s/n): ");
        String confirmacion = scanner.nextLine().trim().toLowerCase();

        if (!confirmacion.equals("s")) {
            System.out.println("Operación cancelada.");
            return;
        }

        String deleteQuery = "DELETE FROM " + nombreTabla + " WHERE " + primaryKey + " = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(deleteQuery)) {
            pstmt.setString(1, valorClavePrimaria);

            int rowsDeleted = pstmt.executeUpdate();
            if (rowsDeleted > 0) {
                System.out.println("Fila eliminada exitosamente con " + primaryKey + ": " + valorClavePrimaria);
            } else {
                System.out.println("No se encontró una fila con " + primaryKey + ": " + valorClavePrimaria);
            }
        } catch (SQLException e) {
            System.out.println("Error al eliminar la fila: " + e.getMessage());
        }
    }

    private static String obtenerClavePrimaria(Connection connection, String nombreTabla) {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet rsPK = metaData.getPrimaryKeys(null, null, nombreTabla.toUpperCase());

            if (rsPK.next()) {
                return rsPK.getString("COLUMN_NAME"); // Nombre de la clave primaria.
            } else {
                System.out.println("La tabla " + nombreTabla + " no tiene clave primaria definida.");
                return null;
            }
        } catch (SQLException e) {
            System.out.println("Error al obtener la clave primaria: " + e.getMessage());
            return null;
        }
    }


    private static void cerrarConexion(Connection connection) {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                System.out.println("Conexión cerrada.");
            }
        } catch (SQLException e) {
            System.out.println("Error al cerrar la conexión: " + e.getMessage());
        }
    }
    
    
}
