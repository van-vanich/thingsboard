package org.thingsboard.server.queue.discovery.consistent;//package org.algo;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.ConcurrentSkipListMap;
//
//public class FindMyBless {
//    private ConcurrentSkipListMap<Integer, Node> listMap = new ConcurrentSkipListMap();
//    private List<Node> nodes = new ArrayList<>();
//    public int solve(List<Data> all){
//
//        if (nodes.isEmpty()) return -1;
//
//        //Сума всієї інформації клієнтів
//        int sumClient = 0;
//
//
//        for (int i=0; i<all.size(); i++) {
//            sumClient += all.get(i).getWeight();
//        }
//        //Мінімальна заповненість ноди
//        int floor = sumClient / nodes.size();
//
//        //Чистимо після минулого разу
//        int sum = 0;
//        for (int i=0; i<nodes.size(); i++) {
//            if (floor > nodes.get(i).getLimit()) return -1;
//            nodes.get(i).cleanBucket();
//            nodes.get(i).setBucket(floor);
//        }
//
//        System.out.println(nodes.size() + " " + listMap.size() + " " + all.size());
//
//        // now - хеш якоїсь ноди зараз
//        // weight - параметр для обходу по колу
//        // cnt - лічильник для зупинки коли не можна розділити
//        // last - останній клієнт
//        // err - чи не зациклилось
//        int now = listMap.higherKey(-1);
//        int weight = 0;
//        int cnt = 0;
//        int last = 0;
//        boolean err = false;
//        int cntReplacing = 0;
//        try {
//            for (int i = 0; i < all.size(); i++) {
//                last = i;
//                cnt++;
//                //перевірка на зациклення
//                if (cnt >= all.size() * 3) {
//                    err = true;
//                    System.out.println("client % cnt.node != 0");
//                    break;
//                }
//                //здвиг другого показника, до потрібного, обхід по часовій стрілці
//                while (listMap.higherKey(now) != null &&
//                        (all.get(i).getKey() - weight < listMap.higherKey(now) && !listMap.get(now).canAddToBucket(all.get(i).getWeight()))) {
//                    now = listMap.higherKey(now);
////                    System.out.println(i);
//                }
////                System.out.println(listMap.get(now).getName() + " " + listMap.get(now).getNowInBucket());
////                System.out.println(weight + " " + now);
//
//                //перевірка, чи потрібно пройти заново по колу
//                if (listMap.higherKey(now) == null) {
//                    weight = Integer.MAX_VALUE;
//                    now = listMap.higherKey(-1);
//                    i--;
//                    continue;
//                }
//                // перевірка чи можна вставити клієнта в цю ноду
//                if (all.get(i).getKey() - weight > listMap.higherKey(now) || !listMap.get(now).canAddToBucket(all.get(i).getWeight())) {
//                    weight = Integer.MAX_VALUE;
//                    i--;
//                } else if (listMap.get(now).canAddToBucket(all.get(i).getWeight())){
//                    // вставка в ноду
//
//
//                    if (all.get(i).getUse() && !all.get(i).getNode().getName().equals(listMap.get(now).getName())) {
//                        cntReplacing++;
//                        System.out.println(all.get(i).getNode().getName() + " => " + listMap.get(now).getName());
//                    }
//
//                    all.get(i).setUse(listMap.get(now));
//
//                    listMap.get(now).addToBucket(all.get(i).getWeight());
////                    System.out.println(all.get(i).getName() + " " +  all.get(i).getWeight() +" == " + all.get(i).getKey() + " => " + now + " " + listMap.get(now).getName() + " " + listMap.get(now).getNowInBucket());
//                }
//            }
////            System.out.println("!!");
//
//            //якщо розставили floor
//            if (last + 1 != all.size() || err) {
//                System.out.println("----------------------");
////                System.out.println();
//                int l = last;
//                for (int i = l; i < all.size(); i++) {
////                    System.out.println("!");
//                    // перевірка на зациклення
//                    cnt++;
//                    last = i;
//                    if (cnt >= all.size() * 6) {
//                        System.out.println("You don't have memory in nodes");
//                        break;
//                    }
//
//                    //здвиг другого показника, до потрібного, обхід по часовій стрілці
////                    System.out.println("can add " + listMap.get(now).canAddToLimit(all.get(i).getWeight()) + " " + listMap.get(now).getNowInBucket());
////                    System.out.println("if use skip " + (listMap.get(now).getNowInBucket() > floor));
////                    System.out.println("if != null " + listMap.higherKey(now) != null);
////                    System.out.println("for kolo " + (all.get(i).getKey() - weight > listMap.higherKey(now)));
//
//                    while (listMap.get(now).getNowInBucket() > floor && listMap.higherKey(now) != null &&
//                            (all.get(i).getKey() - weight < listMap.higherKey(now) || !listMap.get(now).canAddToLimit(all.get(i).getWeight()))) {
//                        now = listMap.higherKey(now);
////                        System.out.println("##");
//                    }
//
////                    System.out.println(listMap.get(now).canAddToLimit(all.get(i).getWeight()));
////                    System.out.println(listMap.get(now).getName() + " " + listMap.get(now).getNowInBucket() + " " + floor);
////                    System.out.println(weight + " " + now);
//
//                    //перевіка на повторний обхід кола і чи вже вставляли в цю ноду лишнього
//                    if (listMap.higherKey(now) == null || listMap.get(now).getNowInBucket() > floor) {
//                        weight = Integer.MAX_VALUE;
//                        now = listMap.higherKey(-1);
//                        i--;
//                        continue;
//                    }
//                    //перевірка чи можна вставити в цю ноду
//                    if (all.get(i).getKey() - weight > listMap.higherKey(now) || !listMap.get(now).canAddToLimit(all.get(i).getWeight())) {
//                        weight = Integer.MAX_VALUE;
//                        i--;
//                    } else if(listMap.get(now).canAddToLimit(all.get(i).getWeight())) {
////                        System.out.println(listMap.get(now).getNowInBucket());
//
//                        //вставка в ноду
//                        if (all.get(i).getUse() && !all.get(i).getNode().getName().equals(listMap.get(now).getName())) {
//                            cntReplacing++;
//                            System.out.println(all.get(i).getNode().getName() + " => " + listMap.get(now).getName());
//                        }
//                        all.get(i).setUse(listMap.get(now));
//                        listMap.get(now).addToBucket(all.get(i).getWeight());
////                        System.out.println(all.get(i).getName() + " " +  all.get(i).getWeight() +" == " + all.get(i).getKey() + " => " + now + " " + listMap.get(now).getName() + " " + listMap.get(now).getNowInBucket());
//                    }
//                }
//            }
//        } catch (NullPointerException e) {
//            System.out.println(e);
//        }
//        //для перевірки на JUnit Test
//
//        if (last + 1  != all.size()) return -1;
//        int answer = 0;
//        for (int i=0; i<nodes.size(); i++) {
//            answer += nodes.get(i).getNowInBucket();
//            System.out.println(nodes.get(i).getName() + " " + nodes.get(i).getNowInBucket());
//        }
//
//        System.out.println("------------------");
//        System.out.println("Element replace is " + cntReplacing);
//        System.out.println("-------------------------------------------------------------------------");
//        return answer;
//
//    }
//
//    //додавання ноди
//    public void addNode(String name, int limit){
//        Node node = new Node(name, limit);
//
//        //вставка на коло хеши ноди
//        for (int j=0; j<node.getHash().size(); j++) {
//            listMap.put(node.getHash().get(j), node);
//        }
//        nodes.add(node);
//    }
//
//    //видалення ноди і її хешів з кола
//    public void deleteNode(String name){
//        for (int i=0; i<nodes.size(); i++) {
//            if (name.equals(nodes.get(i).getName())) {
//                for (int j=0; j < nodes.get(i).getHash().size(); j++) {
//                    listMap.remove(nodes.get(i).getHash().get(j), nodes.get(i));
//                }
//                nodes.remove(i);
//                return;
//            }
//        }
//    }
//
//
//    //хеші нод на колі
//    public void display() {
//
//        int now = listMap.higherKey(-1);
//
//        System.out.println("-----------------");
//        System.out.println(now);
//
//        try {
//            while (!listMap.higherKey(now).equals(null)) {
//                now = listMap.higherKey(now);
//                System.out.println(now);
//            }
//        } catch (NullPointerException e){
//
//        } finally {
//            System.out.println("-----------------------");
//        }
//    }
//
//    public Node getNodeForData(List<Data> all, String name){
//        for (int i=0; i<all.size(); i++) {
//            if (all.get(i).getName().equals(name)) {
//                return all.get(i).getNode();
//            }
//        }
//        return null;
//    }
//}
