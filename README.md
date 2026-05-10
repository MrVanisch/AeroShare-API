# AeroShare API 🚀

AeroShare API to potężne, bezpieczne i ultraszybkie narzędzie do współdzielenia plików typu P2P. Zostało w całości napisane w języku **Rust** przy użyciu asynchronicznego środowiska Tokio i frameworka serwerowego Axum. Zapewnia błyskawiczny transfer ogromnych plików (kilkadziesiąt Gigabajtów) pomiędzy klientami bez obciążania pamięci operacyjnej (RAM) na serwerze przekaźnikowym (Relay).

## Główne cechy (Features) ✨
- **Błyskawiczne Strumieniowanie:** Oparte o architekturę *Relay*. Serwer pośredniczy w wysyłaniu plików między klientami za pomocą niskopoziomowych kanałów (MPSC), przez co nie zużywa RAM-u nawet przy wysyłaniu filmów ważących ponad 100 GB.
- **Dynamiczne Tokeny (Zero-Trust):** Zamiast haseł statycznych system używa generowanego w locie kryptograficznie bezpiecznego tokenu autoryzującego.
- **Ochrona przed Path Traversal:** Pancerne bezpieczeństwo po stronie klienta – głęboka kanonizacja ścieżek `fs::canonicalize()` gwarantuje niemożliwość ucieczki poza wyznaczony folder (zapobiega atakom typu `../../../Windows/`).
- **Komunikacja WebSockets:** Sygnalizacja odbywa się w pełni asynchronicznie, co pozwala systemowi na błyskawiczne reagowanie na żądania pobierania plików.

## Wymagania ⚙️
Aby zbudować i uruchomić ten projekt, potrzebujesz zainstalowanego języka **Rust**.
1. Wejdź na [rustup.rs](https://rustup.rs/) (lub uruchom `rustup-init.exe` z [tej strony](https://win.rustup.rs)).
2. Zainstaluj standardowy Toolchain.

## Architektura
Projekt jest ułożony jako **Cargo Workspace** i składa się z trzech części:
- `server/` – Serwer Sygnalizacyjny i Przekaźnik (Relay). Wystawia port 3000 dla połączeń WebSocket i HTTP.
- `client/` – Lekka aplikacja kliencka udostępniająca pliki w trybie ciągłym.
- `shared/` – Współdzielone modele danych i typy wiadomości.

---

## Konfiguracja i Uruchomienie 🛠️

### 1. Uruchomienie Serwera (Relay)
Uruchom konsolę w głównym katalogu projektu i wpisz:
```bash
cargo run -p server
```
Przy pierwszym uruchomieniu serwer wygeneruje **nowy, bezpieczny token** autoryzacyjny i wyświetli go w konsoli. Zapisze go również w pliku `server_token.txt`.

### 2. Konfiguracja Klienta
Zanim uruchomisz aplikację Klienta, musisz mu podać token oraz wskazać folder do udostępniania.
- Stwórz plik `client_token.txt` w głównym folderze i wklej do niego wygenerowany wcześniej token.
- (Opcjonalnie) Stwórz folder `shared_files` i wrzuć tam jakiekolwiek pliki do udostępnienia (domyślnie użyje `./shared_files`).

### 3. Uruchomienie Klienta
Otwórz drugie okno konsoli i uruchom agenta:
```bash
cargo run -p client
```
Klient połączy się z serwerem po WebSockets (`ws://127.0.0.1:3000/ws`), zautoryzuje się Twoim tokenem i przekaże indeks dostępnych plików. Jeśli inny klient poprosi o plik, ten komputer prześle go strumieniowo HTTP POST bezpośrednio do serwera.

## Zmienne środowiskowe
Zarówno dla serwera jak i klienta możesz sterować procesem za pomocą zmiennych środowiskowych:
* `SERVER_TOKEN` – Omijanie pliku .txt i podanie tokenu z pamięci (dla klienta).
* `SHARED_DIR` – Wskazanie bezwzględnej ścieżki do współdzielonego katalogu, np. `SHARED_DIR="D:\Filmy" cargo run -p client`.
* `RUST_LOG=debug` – Logowanie diagnostyczne.
