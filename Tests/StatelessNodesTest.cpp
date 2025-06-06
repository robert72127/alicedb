#include <iostream>
#include <fstream>
#include <filesystem>
#include <thread>
#include <chrono>

#include "gtest/gtest.h"

#include "AliceDB.h"

struct Person {
    std::array<char, 50> name;
    std::array<char, 50> surname;
    int age;
    float account_balance;
};


struct Name{
    std::array<char,50> name;
};

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


void prepare_test_data_files(std::string people_fname){
    // Seed the random number generator
    std::srand(std::time(nullptr));

    // create file from it

    std::ofstream people_writter{people_fname};
    
    // parse people
    for (auto &name : names){
        for(auto &surname: surnames ){
                int age = std::rand() % 101; // Random number between 0 and 100
                
                float account_ballance =  (std::rand() / (float)RAND_MAX) * 2000.0f;

                std::string person_str = "insert " + std::to_string(AliceDB::get_current_timestamp() ) 
                    + " "  + name + " " + surname + " "  +  std::to_string(age) + " " +std::to_string(account_ballance);
                //std::cout << test_str <<std::endl;
                people_writter << person_str << std::endl;
        }
    }

    people_writter.close();
}


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

//            std::cout << (char*)p << std::endl; 
            return true;
}

void print_person(const AliceDB::Change<Person> &current_change){
        std::cout<<current_change.delta.count << " " << current_change.data.name.data() << " " 
        << current_change.data.surname.data() << " " << current_change.data.age 
        << " " << current_change.data.account_balance  << std::endl; 
} 

void print_name(const AliceDB::Change<Name> &current_change){
    std::cout<<current_change.delta.count << " " << current_change.data.name.data() << std::endl;
} 

TEST(STATELESS_TEST, FILTER){

    std::string people_fname = "people.txt";
    prepare_test_data_files(people_fname);
    
    int worker_threads_cnt = 1;

    auto db = std::make_unique<AliceDB::DataBase>( "./database", worker_threads_cnt);

    auto g = db->CreateGraph();


    auto *view = 
        g->View(
                g->Filter(
                    [](const Person &p) -> bool {return p.age > 18  ;},
                    g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,0)
                )
        );


    db->StartGraph(g);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    db->StopProcessing();


    // debugging

    for(auto it = view->begin() ; it != view->end(); ++it){
        print_person(*it);
    }


    // delete database directory
    db = nullptr;
    std::filesystem::remove_all("database");
}

TEST(STATELESS_TEST, PROJECTION){

    std::string people_fname = "people.txt";
    prepare_test_data_files(people_fname);
    
    int worker_threads_cnt = 1;

    auto db = std::make_unique<AliceDB::DataBase>( "./database", worker_threads_cnt);

    auto g = db->CreateGraph();


    auto *view = 
        g->View(
                g->Projection(
                    [](const Person &p){ return Name{.name=p.name};},
                    g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,0)
                )
        );


    db->StartGraph(g);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    db->StopProcessing();


    // debugging
    
    for(auto it = view->begin() ; it != view->end(); ++it){
        print_name(*it);
    }


    // delete database directory
    db = nullptr;
    std::filesystem::remove_all("database");
}