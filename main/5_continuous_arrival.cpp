#include <iostream>
#include <fstream>
#include <filesystem>
#include <memory>
#include <chrono>
#include <array>
#include <string>

#include "AliceDB.h"

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

struct Person {
    std::array<char, 50> name;
    std::array<char, 50> surname;
    int age;
};

struct Name{
    std::array<char, 50> name;
};



void print_person(const AliceDB::Change<Person> &current_change) {
    std::cout << current_change.delta.count << " " 
              << current_change.data.name.data() << " " 
              << current_change.data.surname.data() << " " 
              << current_change.data.age << std::endl; 
}


void prepare_test_data_file_binary(const std::string &bin_fname) {
    std::ofstream bin_out(bin_fname, std::ios::binary | std::ios::app);
    if (!bin_out.is_open()) {
        throw std::runtime_error("Failed to open file for writing: " + bin_fname);
    }

    int new_data_count = 50 + std::rand() % 100;
    
    for (int i = 0; i < new_data_count; ++i) {
        AliceDB::Change<Person> change;
        std::memset(&change, 0, sizeof(AliceDB::Change<Person>));

        int n = std::rand() % 100;
        int s = std::rand() % 100;

        int positive = std::rand() % 2;
        change.delta.count = -1 + 2*(positive);
        change.delta.ts = AliceDB::get_current_timestamp();

        std::string name_str = names[n % names.size()];
        std::string surname_str = surnames[s % surnames.size()];

        std::strncpy(change.data.name.data(), name_str.c_str(), change.data.name.size());
        std::strncpy(change.data.surname.data(), surname_str.c_str(), change.data.surname.size());
        change.data.age = std::rand() % 100;

        bin_out.write(reinterpret_cast<char*>(&change.delta), sizeof(change.delta));
        bin_out.write(reinterpret_cast<char*>(&change.data), sizeof(Person));
    }
    bin_out.close();
}

void print_names( const AliceDB::Change<Name> &current_change ){
    const Name &n = current_change.data;
    std::cout<<current_change.delta.count << "||";
    std::cout<<n.name.data()  << std::endl; 
} 


int main(){

    std::string bin_fname = "people.bin";
    
    prepare_test_data_file_binary(bin_fname);
    
    auto db = std::make_unique<AliceDB::DataBase>( "./database", 1);

    auto g = db->CreateGraph();

    auto *view = 
            g->View(
                g->Projection(
                    [](const Person &p) {return Name{.name=p.name};},
                    g->Filter(
                        [](const Person &p) -> bool {return p.age > 18 && p.name[0] == 'J';},
                            g->Source<Person>(AliceDB::ProducerType::FILE_BINARY, bin_fname,0.03)
                    )
                )
            );

    db->StartGraph(g);
   
    for(int i = 0; i < 10; i++){
        std::this_thread::sleep_for(std::chrono::seconds(1));
        prepare_test_data_file_binary(bin_fname);
        
        std::cout<<"******************************************\n";
        std::cout<<"|----------------------------------------|\n";
        std::cout<<"|               STATE:                   | \n";
        std::cout<<"|----------------------------------------|\n";
        for(auto it = view->begin() ; it != view->end(); ++it){
            print_names(*it);
        }
        std::cout<<"******************************************\n";
    }
    
    
    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    db->StopProcessing(); 
    

    db = nullptr;
    std::filesystem::remove_all("database");
    int worker_threads_cnt = 1;


}
