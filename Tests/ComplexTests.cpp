#include <iostream>
#include <fstream>
#include <filesystem>
#include <memory>
#include <chrono>

#include "gtest/gtest.h"


#include "AliceDB.h"
// some dummy data

std::array<std::string, 100> surnames = {
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
        "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
        "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
        "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
        "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter", "Roberts",
        "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker", "Cruz", "Edwards", "Collins", "Reyes",
        "Stewart", "Morris", "Morales", "Murphy", "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper",
        "Peterson", "Bailey", "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward", "Richardson",
        "Watson", "Brooks", "Chavez", "Wood", "James", "Bennett", "Gray", "Mendoza", "Ruiz", "Hughes",
        "Price", "Alvarez", "Castillo", "Sanders", "Patel", "Myers", "Long", "Ross", "Foster", "Jimenez"
};

std::array<std::string, 100> names = {
        "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth",
        "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen",
        "Christopher", "Nancy", "Daniel", "Lisa", "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra",
        "Donald", "Ashley", "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
        "Kenneth", "Dorothy", "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa", "Edward", "Deborah",
        "Ronald", "Stephanie", "Timothy", "Rebecca", "Jason", "Sharon", "Jeffrey", "Laura", "Ryan", "Cynthia",
        "Jacob", "Kathleen", "Gary", "Amy", "Nicholas", "Shirley", "Eric", "Angela", "Jonathan", "Helen",
        "Stephen", "Anna", "Larry", "Brenda", "Justin", "Pamela", "Scott", "Emma", "Brandon", "Nicole",
        "Frank", "Samantha", "Benjamin", "Katherine", "Gregory", "Christine", "Samuel", "Debra", "Raymond", "Rachel",
        "Patrick", "Catherine", "Alexander", "Carolyn", "Jack", "Janet", "Dennis", "Ruth", "Jerry", "Maria"
};


std::array<std::string, 50> dogbreeds = {
        "Labrador", "German_Shepherd", "Golden_Retriever", "Bulldog",
        "Beagle", "Poodle", "Rottweiler", "Yorkshire_Terrier", "Boxer", 
        "Dachshund", "Siberian_Husky", "Chihuahua", "Great_Dane", "Shih_Tzu",
        "Doberman_Pinscher", "Australian_Shepherd", "Cocker_Spaniel", "Pomeranian",
        "Boston_Terrier", "Shetland_Sheepdog", "Bernese_Mountain_Dog", "Havanese",
        "Cavalier_King_Charles_Spaniel", "Maltese", "Weimaraner", "Collie", 
        "Bichon_Frise", "English_Springer_Spaniel", "Papillon", "Saint_Bernard",
        "Bullmastiff", "Akita", "Samoyed", "Bloodhound", "Alaskan_Malamute",
        "Newfoundland", "Border_Collie", "Vizsla", "Australian_Cattle_Dog",
        "West_Highland_White_Terrier", "Rhodesian_Ridgeback", "Chow_Chow",
        "Shiba_Inu", "Basset_Hound", "Bulldog", "Irish_Setter",
        "Whippet", "Scottish_Terrier", "Italian_Greyhound", "American_Eskimo_Dog"
    };

std::array<float, 50> dogprices = {
        1000.0, 1200.0, 1100.0, 1300.0,
        900.0, 1500.0, 1400.0, 1250.0, 1000.0, 
        850.0, 2000.0, 800.0, 2500.0, 950.0,
        1350.0, 1450.0, 1050.0, 1150.0, 1000.0, 
        950.0, 1800.0, 1200.0, 1150.0, 900.0,
        1100.0, 1000.0, 1250.0, 1350.0, 1500.0,
        2500.0, 2200.0, 2000.0, 1700.0, 1800.0,
        1600.0, 1400.0, 1250.0, 1100.0, 1300.0,
        1400.0, 1200.0, 950.0, 1100.0, 1500.0,
        1400.0, 1000.0, 900.0, 750.0, 620, 800.0
};

// this isn't required part, but we just need to create our data files
void prepare_people_data_file(std::string people_fname){
    std::srand(std::time(nullptr));

    // cool we can create  100 00 00 unique people
    // create file from it

    std::ofstream people_writter{people_fname};
    
    // parse people
    int cnt = 0;
    for (auto &name : names){
        for(auto &surname: surnames ){
                int age = 50;
                
                int dog_race_nr = 1; 
                
                float account_ballance = 100;

                std::string person_str = "insert " + std::to_string(AliceDB::get_current_timestamp() ) 
                    + " "  + name + " " + surname + " " + dogbreeds[dog_race_nr] + " "  +  std::to_string(age) + " " +std::to_string(account_ballance);
                people_writter << person_str << std::endl;
                cnt++;
                if(cnt > 1){ break;}
        }
        if(cnt > 100){break;}
    }

    people_writter.close();
}

// this isn't required part, but we just need to create our data files
void prepare_people_data_file_random(std::string people_fname){
    std::srand(std::time(nullptr));

    // cool we can create  100 00 00 unique people
    // create file from it

    std::ofstream people_writter{people_fname};
    
    // parse people
    int cnt = 0;
    for (auto &name : names){
        for(auto &surname: surnames ){
                int age = std::rand() % 101; // Random number between 0 and 100
                
                int dog_race_nr = std::rand() % 50;
                
                float account_ballance =  (std::rand() / (float)RAND_MAX) * 2000.0f;

                std::string person_str = "insert " + std::to_string(AliceDB::get_current_timestamp() ) 
                    + " "  + name + " " + surname + " " + dogbreeds[dog_race_nr] + " "  +  std::to_string(age) + " " +std::to_string(account_ballance);
                people_writter << person_str << std::endl;
                cnt++;
                if(cnt > 10){ break;}
        }
        if(cnt > 1000){break;}
    }

    people_writter.close();
}


void prepare_dog_data_file(std::string dogs_fname){

    std::ofstream dog_writter{dogs_fname};
    // parse dogs
    for(int i = 0; i < 5; i++){
        std::string breed = dogbreeds[i];
        float price = dogprices[i];
        std::string dog_str =  "insert " + std::to_string(AliceDB::get_current_timestamp() ) 
                    + " "  +  breed + " "  +  std::to_string(price) ;

        dog_writter << dog_str << std::endl;
    }
    dog_writter.close();
}




// define our input data
struct Person {
    std::array<char, 50> name;
    std::array<char, 50> surname;
    std::array<char, 50> favourite_dog_race;
    int age;
    float account_balance;
};

struct NameTotalBalance {
    std::array<char, 50> name;
    float account_balance;
};

struct Name{
    std::array<char, 50> name;
};

struct Dog {
    std::array<char, 50> name;
    float cost;
};

// this is least nice part, we need to define transitional state structs

struct JoinDogPerson {
    std::array<char, 50> name;
    std::array<char, 50> surname;
    std::array<char, 50> favourite_dog_race;
    float dog_cost;
    float account_balace;
    int age;
};

struct PairPeople {
    std::array<char, 50> lname;
    std::array<char, 50> lsurname;
    int lage;
    std::array<char, 50> rname;
    std::array<char, 50> rsurname;
    int rage;
};


struct SameAgedPeople {
    std::array<char, 50> lname;
    std::array<char, 50> lsurname;
    int age;
    std::array<char, 50> rname;
    std::array<char, 50> rsurname;
};

/// and final state
struct CanAffordDog{
    std::array<char, 50> name;
    std::array<char, 50> surname;
};




void print_person(const AliceDB::Change<Person> &current_change){
    std::cout<<current_change.delta.count << "|" << current_change.delta.ts << "||";
    const Person &p = current_change.data;
    std::cout<<p.name.data() << " " << p.surname.data() << " " << p.favourite_dog_race.data() << " " << p.age << " " << p.account_balance  << std::endl; 
} 

void print_joindogperson( const AliceDB::Change<JoinDogPerson> &current_change ){
    const JoinDogPerson &p = current_change.data;
    std::cout<<current_change.delta.count << "||";
    std::cout<<p.name.data() << " " << p.surname.data() << " " << p.favourite_dog_race.data() << " " << p.dog_cost << " " << p.account_balace << " "  << p.age << std::endl; 
} 

void print_nametotalbalance(const AliceDB::Change<NameTotalBalance> &current_change){
    const NameTotalBalance &p = current_change.data;
    std::cout<<current_change.delta.count << "||";
    std::cout<<p.name.data() << " " << p.account_balance << std::endl; 
} 

void print_canafforddog(const AliceDB::Change<CanAffordDog> &current_change){
    const CanAffordDog &p = current_change.data;
    std::cout<<current_change.delta.count << "||";
    std::cout<<p.name.data() << " " << p.surname.data()  << std::endl; 
} 

void print_pairpeople(const AliceDB::Change<PairPeople> &current_change){
    const PairPeople &p = current_change.data;
    std::cout<<current_change.delta.count << "||";
    std::cout<<p.lname.data() << " " << p.lsurname.data() << " " << p.lage << "\t\t"; 
    std::cout<<p.rname.data() << " " << p.rsurname.data() << " " << p.rage << std::endl; 
} 

void print_sameagedpeople(const AliceDB::Change<SameAgedPeople> &current_change){
    const SameAgedPeople &p = current_change.data;
    std::cout<<current_change.delta.count << "||";
    std::cout<<p.lname.data() << " " << p.lsurname.data() << " " << p.age << " "<< p.rname.data() << " " << p.rsurname.data()  << std::endl; 
} 



// we need to provide parseLine function to specify how to parse structs from producer into source, we might change api

// note this could be simplified with reflection mechanism 
bool parsePerson(std::istringstream &iss, Person *p) {

            char name[50];
            char surname[50];
            char favourite_dog_race[50];

            if (!(iss >> name >> surname >> favourite_dog_race >> p->age >> p->account_balance )) {
                return false; // parse error
            }

            // Copy fields to ensure no overflow:
            std::strncpy(p->name.data(), name, sizeof(p->name));
            std::strncpy(p->surname.data(), surname, sizeof(p->surname));
            std::strncpy(p->favourite_dog_race.data(), favourite_dog_race, sizeof(p->favourite_dog_race));

            return true;
}

bool parseDog(std::istringstream &iss, Dog *d) {

            char name[50];

            if (!(iss >> name >> d->cost )) {
                return false; // parse error
            }

            // Copy fields to ensure no overflow:
            std::strncpy(d->name.data(), name, sizeof(d->name));
            return true;
}


TEST(MULTINODETEST, INSERTS){

    std::string people_fname = "people.txt";
    prepare_people_data_file_random(people_fname);
     std::string dogs_fname = "dogs.txt";
    prepare_dog_data_file(dogs_fname);


    int worker_threads_cnt = 1;

    auto db = std::make_unique<AliceDB::DataBase>( "./database", worker_threads_cnt);

    auto g = db->CreateGraph();

    auto *view = 
        g->View(
            g->Projection(
                [](const JoinDogPerson &p) {
                    return CanAffordDog{
                       .name=p.name,
                       .surname=p.surname, 
                    };
                },
                g->Filter(
                    [](const JoinDogPerson &p) -> bool {return p.account_balace > p.dog_cost;},
                    g->Join(
                        [](const Person &p)  { return p.favourite_dog_race;},
                        [](const Dog &d)  { return d.name;},
                        [](const Person &p, const Dog &d) { 
                            return  JoinDogPerson{
                                .name=p.name,
                                .surname=p.surname,
                                .favourite_dog_race=d.name,
                                .dog_cost=d.cost,
                                .account_balace=p.account_balance,
                                .age=p.age
                            };
                        },
                        g->Filter(
                            [](const Person &p) -> bool {return p.age > 18;},
                            g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,0)
                        ),
                        g->Source<Dog>(AliceDB::ProducerType::FILE, dogs_fname, parseDog,0)
                    )
                    
                )
            )
        );

    db->StartGraph(g);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    db->StopGraph(g);


    for(auto it = view->begin() ; it != view->end(); ++it){
        print_canafforddog(*it);
    }

    // delete database directory
    db = nullptr;
    std::filesystem::remove_all("database");
}


// this isn't required part, but we just need to create our data files
void prepare_people_data_delete_test(std::string people_fname){
    std::srand(std::time(nullptr));

    // cool we can create  100 00 00 unique people
    // create file from it

    std::ofstream people_writter{people_fname};
    
    int age = std::rand() % 101; 
                
    int dog_race_nr = std::rand() % 50;
                
    float account_ballance =  (std::rand() / (float)RAND_MAX) * 2000.0f;

    std::string name = "Joahmin";

    std::string surname = "Smith";

    std::string person_str = "insert " + std::to_string(AliceDB::get_current_timestamp() ) 
                    + " "  + name + " " + surname + " " + dogbreeds[dog_race_nr] + " "  +  std::to_string(age) + " " +std::to_string(account_ballance);
    
    people_writter << person_str << std::endl;
    people_writter << person_str << std::endl;
    
    person_str = "delete " + std::to_string(AliceDB::get_current_timestamp() ) 
                    + " "  + name + " " + surname + " " + dogbreeds[dog_race_nr] + " "  +  std::to_string(age) + " " +std::to_string(account_ballance);

    people_writter << person_str << std::endl;
    people_writter << person_str << std::endl;
    people_writter << person_str << std::endl;
    
    
    people_writter.close();
}

// first insert then delete some change, make sure that count is correct
TEST(DELETE_TEST, DELETE){
    std::string people_fname = "people.txt";
    prepare_people_data_delete_test(people_fname);
     std::string dogs_fname = "dogs.txt";


    int worker_threads_cnt = 1;

    auto db = std::make_unique<AliceDB::DataBase>( "./database", worker_threads_cnt);

    auto g = db->CreateGraph();

    auto *view = 
        g->View(
            g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,0)
        );

    db->StartGraph(g);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    db->StopGraph(g);


    for(auto it = view->begin() ; it != view->end(); ++it){
        print_person(*it);
        AliceDB::Delta d = (*it).delta;
        ASSERT_LE ( d.count, 0);
    }

    // delete database directory
    db = nullptr;
    std::filesystem::remove_all("database");

}


// first insert then delete some change, make sure that count is correct
TEST(DELETE_TEST, DELETE_DISTINCT){

    std::string people_fname = "people.txt";
    prepare_people_data_delete_test(people_fname);


    int worker_threads_cnt = 1;

    auto db = std::make_unique<AliceDB::DataBase>( "./database", worker_threads_cnt);

    auto g = db->CreateGraph();

    auto *view = 
        g->View(
            g->Distinct(
                g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,0)
            )
        );

    db->StartGraph(g);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    db->StopGraph(g);


    int cnt = 0;
    for(auto it = view->begin() ; it != view->end(); ++it){
        cnt++;
    }
    ASSERT_EQ(cnt, 0);

    // delete database directory
    db = nullptr;
    std::filesystem::remove_all("database");
}


void prepare_people_data_delete_test_future(std::string people_fname){
    std::srand(std::time(nullptr));

    // cool we can create  100 00 00 unique people
    // create file from it

    std::ofstream people_writter{people_fname};
    
    int age = std::rand() % 101; // Random number between 0 and 100
                
    int dog_race_nr = std::rand() % 50;
                
    float account_ballance =  (std::rand() / (float)RAND_MAX) * 2000.0f;

    std::string name = "Joahmin";

    std::string surname = "Smith";

    std::string person_str = "insert " + std::to_string(AliceDB::get_current_timestamp() ) 
                    + " "  + name + " " + surname + " " + dogbreeds[dog_race_nr] + " "  +  std::to_string(age) + " " +std::to_string(account_ballance);
    
    people_writter << person_str << std::endl;
    people_writter << person_str << std::endl;
    
    person_str = "delete " + std::to_string( (AliceDB::get_current_timestamp() * 2) ) 
                    + " "  + name + " " + surname + " " + dogbreeds[dog_race_nr] + " "  +  std::to_string(age) + " " +std::to_string(account_ballance);

    people_writter << person_str << std::endl;
    people_writter << person_str << std::endl;
    people_writter << person_str << std::endl;
    
    
    people_writter.close();
}

// insert and delete change with different times, check if view is correct at each time
// first insert then delete some change, make sure that count is correct
TEST(DELETE_TEST, DELETE_IN_FUTURE){
    std::string people_fname = "people.txt";
    prepare_people_data_delete_test_future(people_fname);
     std::string dogs_fname = "dogs.txt";


    int worker_threads_cnt = 1;

    auto db = std::make_unique<AliceDB::DataBase>( "./database", worker_threads_cnt);

    auto g = db->CreateGraph();

    auto *view = 
        g->View(
            g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,0)
        );

    db->StartGraph(g);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    db->StopGraph(g);


    for(auto it = view->begin() ; it != view->end(); ++it){
        print_person(*it);
        AliceDB::Delta d = (*it).delta;
        ASSERT_EQ ( d.count, 2);
    }

    // delete database directory
    db = nullptr;
    std::filesystem::remove_all("database");
}

// insert and delete change with different times, check if view is correct at each time
// first insert then delete some change, make sure that count is correct
TEST(DELETE_TEST, DELETE_DISTINCT_IN_FUTURE){

    std::string people_fname = "people.txt";
    prepare_people_data_delete_test_future(people_fname);


    int worker_threads_cnt = 1;

    auto db = std::make_unique<AliceDB::DataBase>( "./database", worker_threads_cnt);

    auto g = db->CreateGraph();

    auto *view = 
        g->View(
            g->Distinct(
                g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,0)
            )
        );

    db->StartGraph(g);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    db->StopGraph(g);


    for(auto it = view->begin() ; it != view->end(); ++it){
        AliceDB::Delta d = (*it).delta;
        ASSERT_EQ ( d.count, 1);
    }

    // delete database directory
    db = nullptr;
    std::filesystem::remove_all("database");
}


void prepare_people_data_delete_test_non_zero_frontier(std::string people_fname){
    std::srand(std::time(nullptr));

    // cool we can create  100 00 00 unique people
    // create file from it

    std::ofstream people_writter{people_fname};
    
    int age = std::rand() % 101; // Random number between 0 and 100
                
    int dog_race_nr = std::rand() % 50;
                
    float account_ballance =  (std::rand() / (float)RAND_MAX) * 2000.0f;

    std::string name = "Joahmin";

    std::string surname = "Smith";

    std::string person_str = "insert " + std::to_string(AliceDB::get_current_timestamp() ) 
                    + " "  + name + " " + surname + " " + dogbreeds[dog_race_nr] + " "  +  std::to_string(age) + " " +std::to_string(account_ballance);
    
    people_writter << person_str << std::endl;
    
    person_str = "delete " + std::to_string( (AliceDB::get_current_timestamp() + 3) ) 
                    + " "  + name + " " + surname + " " + dogbreeds[dog_race_nr] + " "  +  std::to_string(age) + " " +std::to_string(account_ballance);

    people_writter << person_str << std::endl;
    
    people_writter.close();
}

TEST(DELETE_TEST, DELETE_NON_ZERO_FRONTIER){

    std::string people_fname = "people.txt";
    prepare_people_data_delete_test_non_zero_frontier(people_fname);

    int worker_threads_cnt = 1;

    auto db = std::make_unique<AliceDB::DataBase>( "./database", worker_threads_cnt);

    auto g = db->CreateGraph();

    auto *view = 
        g->View(
            g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,2)
        );

    db->StartGraph(g);
    std::this_thread::sleep_for(std::chrono::seconds(4));
    db->StopGraph(g);


    for(auto it = view->begin() ; it != view->end(); ++it){
        print_person(*it);
        AliceDB::Delta d = (*it).delta;
        ASSERT_EQ ( d.count, 1);
    }

    // delete database directory
    db = nullptr;
    std::filesystem::remove_all("database");


}


void prepare_data_garbage_collection_test(std::string people_fname){
    std::srand(std::time(nullptr));

    // cool we can create  100 00 00 unique people
    // create file from it

    std::ofstream people_writter{people_fname};
    
    // parse people
    
    // insert enough to fill single page
    int max = 4096;

    int cnt = 0;
    for (auto &name : names){
        for(auto &surname: surnames ){
                int age =  std::rand() % 101; // Random number between 0 and 100
                
                int dog_race_nr = 1; 
                
                float account_ballance = 100;

                // half of changes will be very old
                std::string person_str = "insert " + std::to_string(AliceDB::get_current_timestamp() / (1 + cnt % 2)  ) 
                    + " "  + name + " " + surname + " " + dogbreeds[dog_race_nr] + " "  +  std::to_string(age) + " " +std::to_string(account_ballance);
                
                people_writter << person_str << std::endl;
                cnt++;
                if(cnt >= max){ break;}
        }
        if(cnt >= max){break;}
    }

    people_writter.close();
}


// insert enough to fill K Pages, then perform garbage collection,
// after that insert few new changes, check if they were inserted into empty space, created by garbage collection
TEST(GARBAGE_COLLECTION_TEST, GARBAGE_COLLECTION){

    std::string people_fname = "people.txt";
    prepare_data_garbage_collection_test(people_fname);
     std::string dogs_fname = "dogs.txt";


    int worker_threads_cnt = 1;



    auto *gb_settings = new AliceDB::GarbageCollectSettings{
        .clean_freq_ = 1,
        .delete_age_ = 150,
        .use_garbage_collector = true,
        .remove_zeros_only = false
    };


    auto db = std::make_unique<AliceDB::DataBase>( "./database", worker_threads_cnt, gb_settings);

    auto g = db->CreateGraph();

    auto *view = 
        g->View(
            g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,0)
        );

    db->StartGraph(g);
    std::this_thread::sleep_for(std::chrono::seconds(3));
    db->StopGraph(g);


    int cnt = 0;
    for(auto it = view->begin() ; it != view->end(); ++it){
        print_person(*it);
        cnt++;
    }
    ASSERT_EQ(cnt, 4096/2);
    std::cout<<cnt<<std::endl;

    // delete database directory
    db = nullptr;
    std::filesystem::remove_all("database");

}
