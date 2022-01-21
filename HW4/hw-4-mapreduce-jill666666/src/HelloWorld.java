



public class HelloWorld {
    // Your program begins with a call to main().
    // Prints "Hello, World" to the terminal window.
    public static void main(String args[]) {
        System.out.println("Hello, World");

        String arrayString = "[]";
        arrayString = arrayString.replace("[", "").replace("]", "");
        System.out.println("array string is ");
        System.out.println(arrayString);
        if (arrayString.isEmpty()) {
            System.out.print("empty array");
        }
        // System.out.println(Integer.parseInt(arrayString));

        int[] emptyList = new int[1];
        System.out.println(emptyList.length);
    }
}