import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;




public class TransactionProcessorSample {

    public static void main(final String[] args) throws IOException {

        List<User> users = TransactionProcessorSample.readUsers(Paths.get(args[0]));
        List<Transaction> transactions = TransactionProcessorSample.readTransactions(Paths.get(args[1]));
        List<BinMapping> binMappings = TransactionProcessorSample.readBinMappings(Paths.get(args[2]));
        List<Event> events = TransactionProcessorSample.processTransactions(users, transactions, binMappings);

        TransactionProcessorSample.writeBalances(Paths.get(args[3]), users);
        TransactionProcessorSample.writeEvents(Paths.get(args[4]), events);



    }
    // Creating a list of users.
    private static List<User> readUsers(final Path filePath) throws IOException {
       List<User> listOfUsers = new ArrayList<>();

        BufferedReader reader = new BufferedReader(new FileReader(String.valueOf(filePath)));

        String l;
        while ((l = reader.readLine()) != null) {
            List<String> temps = Arrays.asList(l.split(","));

            if (temps.get(0).equals("USER_ID")) continue;

            try {
                listOfUsers.add(new User(temps.get(0),
                        temps.get(1),
                        new BigDecimal(temps.get(2)),
                        temps.get(3),
                        Integer.parseInt(temps.get(4)),
                        new BigDecimal(temps.get(5)),
                        new BigDecimal(temps.get(6)),
                        new BigDecimal(temps.get(7)),
                        new BigDecimal(temps.get(8))));

            } catch (NumberFormatException e) {

                System.err.println("Error processing transaction. Seems to be a problem with the numbers" + l);
            }
        }



        return listOfUsers;
    }
    // Creating a list of transactions.
    private static List<Transaction> readTransactions(final Path filePath) throws IOException {
        List<Transaction> listOfTransactions = new ArrayList<>();

        BufferedReader reader = new BufferedReader(new FileReader(filePath.toFile()));
            String line;
            while ((line = reader.readLine()) != null) {

                List<String> temps = Arrays.asList(line.split(","));

                if (temps.get(0).equals("TRANSACTION_ID")) continue;

                try {
                    Transaction transaction = new Transaction(temps.get(0),
                            temps.get(1),
                            temps.get(2),
                            new BigDecimal(temps.get(3)),
                            temps.get(4),
                            temps.get(5));

                    listOfTransactions.add(transaction);
                } catch (NumberFormatException e) {

                    System.err.println("Error processing transaction. Amount is not a digit: " + line);

                }


            }return listOfTransactions;
    }
    // Creating a list of binMappings.
    private static List<BinMapping> readBinMappings(final Path filePath) throws IOException {

        List<BinMapping> listOfBinMappings = new ArrayList<>();

        String l;

        BufferedReader reader = new BufferedReader(new FileReader(String.valueOf(filePath)));

        while ((l = reader.readLine()) != null) {
            List<String> temps = Arrays.asList(l.split(","));
            if(temps.get(0).equals("NAME")) continue;
            try {
            listOfBinMappings.add(new BinMapping(temps.get(0),
                    Long.parseLong(temps.get(1)),
                    Long.parseLong(temps.get(2)),
                    temps.get(3),
                    temps.get(4)));
            } catch (NumberFormatException e) {

                System.err.println("Error processing transaction. Seems to be a problem with the numbers"  + l);

            }


        }

        return listOfBinMappings;

    }
    // Divide transaction types and check various things.
    private static List<Event> processTransactions(final List<User> users, final List<Transaction> transactions, final List<BinMapping> binMappings) {

        List<Event> events = new ArrayList<>();
        List <String> successfulDeposits = new ArrayList<>();
        final Map <String, String> usedCombinations = new HashMap<>();


        for ( Transaction t : transactions) {
            try {
                if (isTransactionUnique(events,t) // Checking if transaction is unique and has not been used before.
                        && doesUserExist(users,t,events)  // Checking if user exists.
                        && isUserNotFrozen(users,t,events) // Checking if user is not frozen.
                        && t.amount.compareTo(BigDecimal.ZERO)>0 // Checking amount is a valid (positive).
                        && isUserWithinLimits(users,t,events) // Checking that amount is within deposit/withdraw limits.
                        && isThereDualUsage(usedCombinations,t,events)) // Checking that users are not sharing iban/card;
                {
                    if (t.method.equals("TRANSFER")) {
                        if(isValidIBAN(t.accountNumber,events,t) && isAccountCountrySame(t,users,events)) // Checking IBAN digit validity.
                        {
                            if (t.type.equals("DEPOSIT")) {
                                depositMoney(users, t, events);
                                successfulDeposits.add(t.accountNumber);
                                usedCombinations.put(t.accountNumber,t.userId);
                            }
                            if (t.type.equals("WITHDRAW")) {
                                withdrawMoney(users,t,events,successfulDeposits, binMappings);
                                 }
                                usedCombinations.put(t.accountNumber,t.userId);
                        }
                    }


                    if (t.method.equals("CARD")
                            && cardCountryCheck(binMappings,t,users, events)) // Checking that card country matches user country.
                    {

                        if (t.type.equals("DEPOSIT")&& cardTypeCheck(binMappings,t,events)) // Checking that card type is a debit card.
                        {
                            depositMoney(users, t, events);
                            successfulDeposits.add(t.accountNumber);
                            usedCombinations.put(t.accountNumber,t.userId);
                        }
                        if (t.type.equals("WITHDRAW")) {

                            withdrawMoney(users,t,events,successfulDeposits,binMappings);
                            usedCombinations.put(t.accountNumber,t.userId);

                        }

                        if (!Objects.equals(t.type, "DEPOSIT") && !Objects.equals(t.type, "WITHDRAW")) {
                            events.add(new Event(t.transactionId, Event.STATUS_DECLINED, "Transaction type not valid " + t.type));
                        }

                    }
                }
            } catch (Exception e) {

                System.err.println("Error processing transaction: " + e.getMessage());
            }
        }

        return events;
    }
    // Checking if transaction account country matches user country.
    private static boolean isAccountCountrySame(Transaction t, List<User> users, List<Event> events) {
        boolean answer = false;

        for (User u : users) {
            if (u.userId.equals(t.userId)) {
                if (u.countryCode.equals(t.accountNumber.substring(0, 2))) {
                    answer = true;
                } else
                    events.add(new Event(t.transactionId, Event.STATUS_DECLINED, "Invalid account country " + t.accountNumber.substring(0, 2) + "; expected " + u.countryCode));
            }
        }

        return answer;
    }

    // Checking if IBAN is valid.
    private static boolean isValidIBAN(String iban, List<Event> events, Transaction t) {
        iban = iban.replaceAll("\\s", "").toUpperCase();

        if (iban.length() < 4 || !iban.matches("[A-Z0-9]+")) {
            return false;
        }

        String countryCode = iban.substring(0, 2);
        String checkDigits = iban.substring(2, 4);
        String restOfIban = iban.substring(4);

        String rearrangeIban = restOfIban + countryCode + checkDigits;
        StringBuilder ibanInNumbers = new StringBuilder();

        for (char c : rearrangeIban.toCharArray()) {
            if (Character.isDigit(c)) {
                ibanInNumbers.append(c);
            } else {
                ibanInNumbers.append(10 + (c - 'A'));
            }
        }

        BigInteger ibanNumber = new BigInteger(ibanInNumbers.toString());

        boolean answer = ibanNumber.mod(BigInteger.valueOf(97)).intValue() == 1;

        if (!answer) {events.add(new Event(t.transactionId, Event.STATUS_DECLINED, "Invalid iban "+iban));}

        return answer;
    }


    // Checking that users are not sharing iban/card. Payment account used by one user can no longer be used by another.
    private static boolean isThereDualUsage(Map <String, String> usedCombinations, Transaction t, List<Event> events){
        boolean answer = true;

        if (usedCombinations.containsKey(t.accountNumber)){
            if(!usedCombinations.get(t.accountNumber).equals(t.userId)){

                answer = false;
                events.add(new Event(t.transactionId, Event.STATUS_DECLINED, "Account " +t.accountNumber+ " is in use by other user"));
            }

        }

        return answer;
    }
    // Checking that the transaction ID is unique (not used before).
    private static boolean isTransactionUnique (List<Event> events, Transaction transaction) {
        boolean answer = true;
        for (Event e : events) {

            if (e.transactionId.equals(transaction.transactionId)) {
                answer = false;
                break;
            }
        }
        if (!answer) {
            events.add(new Event(transaction.transactionId, Event.STATUS_DECLINED, "Transaction "+transaction.transactionId+ " already processed (id non-unique)"));
        }

        return answer;
    }

    // Checking if amount is within deposit/withdraw limits.
    private static boolean isUserWithinLimits (final List<User> users, Transaction transaction, List<Event> events) {

        boolean answer = false;

        String method = transaction.type;


        User temp;
        for (User u : users) {
            if(u.userId.equals(transaction.userId)) {
                temp = u;
                switch (method) {
                    case "DEPOSIT":

                        if (transaction.amount.compareTo(u.depositMin) >= 0 && transaction.amount.compareTo(u.depositMax) <= 0) {
                            answer = true;
                        } else if (transaction.amount.compareTo(temp.depositMin)<0) {
                            events.add(new Event(transaction.transactionId, Event.STATUS_DECLINED, "Amount " + transaction.amount + " is under the deposit limit of " + temp.depositMin));
                        } else if (transaction.amount.compareTo(temp.depositMax)>0) {
                            events.add(new Event(transaction.transactionId, Event.STATUS_DECLINED, "Amount " + transaction.amount + " is over the deposit limit of " + temp.depositMax));
                        }
                        break;

                        case "WITHDRAW":

                            if (transaction.amount.compareTo(u.withdrawMin) >= 0 && transaction.amount.compareTo(u.withdrawMax)<= 0) {
                                answer = true;
                                } else if (transaction.amount.compareTo(temp.withdrawMin)<0) {
                                events.add(new Event(transaction.transactionId, Event.STATUS_DECLINED, "Amount " + transaction.amount + " is under the withdraw limit of " + temp.withdrawMin));
                            } else if (transaction.amount.compareTo(temp.withdrawMax)>0) {
                                events.add(new Event(transaction.transactionId, Event.STATUS_DECLINED, "Amount " + transaction.amount + " is over the withdraw limit of " + temp.withdrawMax));
                            }
                                break;

                            default: events.add(new Event(transaction.transactionId, Event.STATUS_DECLINED, "Transaction type: "+transaction.type+" is not valid. Expected transaction types: DEPOSIT or WITHDRAW"));

                        }
                    }
                }

        return answer;
    }
    // Checking that user from transaction exists in User list.
    private static boolean doesUserExist(final List<User> users, Transaction transaction, List<Event> events) {
        boolean answer = false;
        for (User u : users) {

            if(u.userId.equals(transaction.userId)) {
                answer = true; break;
            }
        }
        if (!answer) {events.add(new Event(transaction.transactionId, Event.STATUS_DECLINED, "User "+transaction.userId+" not found in Users"));}
        return answer;
    }
    // Checking that the user is not frozen.
    private static boolean isUserNotFrozen (final List<User> users, Transaction transaction, List<Event> events) {

        int frozen = 1;
        boolean answer = false;
        for (User u : users) {

            if(u.userId.equals(transaction.userId)) {
                frozen = u.frozen;
            }
        }
        if (frozen == 0) {answer = true;}
        if (frozen == 1) {events.add(new Event(transaction.transactionId, Event.STATUS_DECLINED, "User "+transaction.userId+" is frozen!"));}
        return answer;
    }

    // Withdrawing money.
    private static void withdrawMoney (final List<User> users, Transaction transaction, List<Event> events,List<String> successfulDeposits,List<BinMapping> binMappings) {


        for (User u : users) {
            if (u.userId.equals(transaction.userId)) {
                if (u.balance.compareTo(transaction.amount) >= 0) {
                    if (successfulDeposits.contains(transaction.accountNumber)) {
                    if (cardTypeCheck(binMappings,transaction,events)){
                    u.balance = u.balance.subtract(transaction.amount);

                    events.add(new Event(transaction.transactionId, Event.STATUS_APPROVED, "OK"));
                    break; }
                    } else { events.add(new Event(transaction.transactionId, Event.STATUS_DECLINED, "Cannot withdraw with a new account " + transaction.accountNumber));}
                } else {
                    events.add(new Event(transaction.transactionId, Event.STATUS_DECLINED, "Not enough balance to withdraw " + transaction.amount + " - balance is too low at " + u.balance));
                }

            }
        }
    }
    // Depositing money.
    private static void depositMoney (final List<User> users, Transaction transaction, List<Event> events) {

        for (User u : users) {
            if(u.userId.equals(transaction.userId)) {
                u.balance = u.balance.add(transaction.amount);
                events.add(new Event(transaction.transactionId,Event.STATUS_APPROVED,"OK"));
            }
        }
    }


    // Checking from binMappings if DC card is used for transactions.
    private static boolean cardTypeCheck (final List<BinMapping> binMappings, Transaction transaction, List<Event> events) {

        if (transaction.method.equals("TRANSFER")){ return true; }
        boolean answer = false;
        String cardType = null;

        for (BinMapping bm : binMappings ) {
            long accountNo = Long.parseLong(transaction.accountNumber.substring(0,10));

            if (accountNo>=bm.rangeFrom
                    && accountNo<=bm.rangeTo){

                cardType = bm.type;

            }
        }
        assert cardType != null;
        if (cardType.equals("DC")) {answer = true;}
        if (!cardType.equals("DC")) { events.add(new Event(transaction.transactionId, Event.STATUS_DECLINED, "Only DC cards allowed; got " + cardType));

        }

        return answer;
    }

    // Checking if user country and card country match.
    private static boolean cardCountryCheck (List<BinMapping> binMappings, Transaction transaction, final List<User> users, List<Event> events) {
        boolean answer = false;
        String transactionCountryCode = null;
        String userCountryCode = null;

        for (BinMapping bm : binMappings ) {
            long accountNo = Long.parseLong(transaction.accountNumber.substring(0,10));

                if (accountNo >= bm.rangeFrom && accountNo <= bm.rangeTo) {

                    transactionCountryCode = bm.countryCode;}

        }
        for (User u : users) {
            if (u.userId.equals(transaction.userId)) {
                userCountryCode = u.countryCode;

            }
        }

        assert transactionCountryCode != null;
        if (transactionCountryCode.equals(countryCodeSwap(userCountryCode))) {
            answer = true;

        } else {

            events.add(new Event(transaction.transactionId, Event.STATUS_DECLINED, "Invalid country " +transactionCountryCode+ "; expected "+userCountryCode+"("+countryCodeSwap(userCountryCode)+")"));

        }

        return answer;

    }
    // Turning 2-letter country code into 3-letter country code.
    private static String countryCodeSwap(String iso2CountryCode){

        Locale locale = new Locale.Builder().setRegion(iso2CountryCode).build();
        return locale.getISO3Country();
    }


    // Writing balances
    private static void writeBalances(final Path filePath, final List<User> users) throws IOException {
        try (final FileWriter writer = new FileWriter(filePath.toFile(), false)) {
            writer.append("user_id,balance\n");
            for (final var user : users) {
                writer.append(user.userId).append(",").append(user.balance.toString()).append("\n");


            }
        }
    }

    // Writing events.
    private static void writeEvents(final Path filePath, final List<Event> events) throws IOException {
        try (final FileWriter writer = new FileWriter(filePath.toFile(), false)) {
            writer.append("transaction_id,status,message\n");
            for (final var event : events) {
                writer.append(event.transactionId).append(",").append(event.status).append(",").append(event.message).append("\n");
            }
        }
    }
}


class User {

    public String userId;
    public String userName;
    public BigDecimal balance;
    public String countryCode;
    public int frozen;
    public BigDecimal depositMin;
    public BigDecimal depositMax;
    public BigDecimal withdrawMin;
    public BigDecimal withdrawMax;


    public User(String userId, String userName, BigDecimal balance, String countryCode, int frozen, BigDecimal depositMin, BigDecimal depositMax, BigDecimal withdrawMin, BigDecimal withdrawMax) {
        this.userId = userId;
        this.userName = userName;
        this.balance = balance;
        this.countryCode = countryCode;
        this.frozen = frozen;
        this.depositMin = depositMin;
        this.depositMax = depositMax;
        this.withdrawMin = withdrawMin;
        this.withdrawMax = withdrawMax;
    }



    @Override
    public String toString() {
        return "User{" +
                "userId=" + userId +
                ", userName='" + userName + '\'' +
                ", balance=" + balance +
                ", countryCode='" + countryCode + '\'' +
                ", frozen=" + frozen +
                ", depositMin=" + depositMin +
                ", depositMax=" + depositMax +
                ", withdrawMin=" + withdrawMin +
                ", withdrawMax=" + withdrawMax +
                '}';
    }
}

class Transaction {

    public String transactionId;
    public String userId;
    public String type;
    public BigDecimal amount;
    public String method;

    public String accountNumber;

    public Transaction(String transactionId, String userId, String type, BigDecimal amount, String method, String accountNumber) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.type = type;
        this.amount = amount;
        this.method = method;
        this.accountNumber = accountNumber;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "transactionId='" + transactionId + '\'' +
                ", userId='" + userId + '\'' +
                ", type='" + type + '\'' +
                ", amount=" + amount +
                ", method='" + method + '\'' +
                ", accountNumber='" + accountNumber + '\'' +
                '}';
    }
}

class BinMapping {

    public String bankName;
    public long rangeFrom;
    public long rangeTo;
    public String type;

    public String countryCode;



    public BinMapping(String bankName, long rangeFrom, long rangeTo, String type, String countryCode) {
        this.bankName = bankName;
        this.rangeFrom = rangeFrom;
        this.rangeTo = rangeTo;
        this.type = type;
        this.countryCode = countryCode;
    }

    @Override
    public String toString() {
        return "BinMapping{" +
                "bankName='" + bankName + '\'' +
                ", rangeFrom='" + rangeFrom + '\'' +
                ", rangeTo='" + rangeTo + '\'' +
                ", type='" + type + '\'' +
                ", countryCode='" + countryCode + '\'' +
                '}';
    }
}

class Event {
    public static final String STATUS_DECLINED = "DECLINED";
    public static final String STATUS_APPROVED = "APPROVED";

    public String transactionId;
    public String status;
    public String message;

    public Event(String transactionId, String status, String message) {
        this.transactionId = transactionId;
        this.status = status;
        this.message = message;
    }
}
