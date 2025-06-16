#include <iostream>
#include <fstream>
#include <filesystem>
#include <memory>
#include <chrono>

#include "AliceDB.h"

std::array<std::string, 30> surnames = {
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
        "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
        "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
       };

std::array<std::string, 30> names = {
        "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth",
        "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen",
        "Christopher", "Nancy", "Daniel", "Lisa", "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra"};

void prepare_people_data_file(std::string people_fname){

    std::srand(std::time(nullptr));

    std::ofstream people_writter{people_fname};
    
    int cnt = 0;
    for (auto &name : names){
        for(auto &surname: surnames ){
                int age = std::rand() % 101;
                
                float account_ballance =  (std::rand() / (float)RAND_MAX) * 800.0f;

                std::string person_str = "insert " + std::to_string(AliceDB::get_current_timestamp() ) 
                    + " "  + name + " " + surname + " "  + " "  +  std::to_string(age) + " " +std::to_string(account_ballance);
                people_writter << person_str << std::endl;
                cnt++;
        }
    }

    people_writter.close();
}

struct Person {
    std::array<char, 50> name;
    std::array<char, 50> surname;
    int age;
    float account_balance;
};

struct NameBalance{
    std::array<char, 50> name;
    float account_balance;
};


bool parsePerson(std::istringstream &iss, Person *p) {

            char name[50];
            char surname[50];

            if (!(iss >> name >> surname >> p->age >> p->account_balance )) {
                return false;
            }

            std::strncpy(p->name.data(), name, sizeof(p->name));
            std::strncpy(p->surname.data(), surname, sizeof(p->surname));

            return true;
}

void print_name_balance( const AliceDB::Change<NameBalance> &current_change ){
    const NameBalance &n = current_change.data;
    if(current_change.delta.count > 0){
        std::cout<<current_change.delta.count << "||";
        std::cout<<n.name.data() << " || " << n.account_balance  << std::endl; 
    }
} 

int main(){
    
    std::string people_fname = "people.txt";
    prepare_people_data_file(people_fname);


    bool stop = false;

    int worker_threads_cnt = 1;
    
    auto db = std::make_unique<AliceDB::DataBase>( "./database", worker_threads_cnt);

    auto g = db->CreateGraph();


    auto *view = 
        g->View(
                g->AggregateBy(
                    [](const Person &p) { return  p.name; },
                    [](const Person &p, int count, const NameBalance &nb, bool first ){
                        return NameBalance {
                            .name = p.name,
                            .account_balance = first?  p.account_balance : std::max( p.account_balance, nb.account_balance)
                        };
                    },
                    g->Source<Person>(AliceDB::ProducerType::FILE , people_fname, parsePerson,0.05)
                )
        );

    db->StartGraph(g);
   
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    db->StopProcessing(); 
    
    for(auto it = view->begin() ; it != view->end(); ++it){
        print_name_balance(*it);
    }
    

    db = nullptr;
    std::filesystem::remove_all("database");
}
