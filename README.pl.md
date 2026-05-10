# AeroShare API

AeroShare API to aplikacja do udostepniania plikow przez serwer relay. Projekt jest napisany w Rust i sklada sie z trzech czesci:

- `server` - serwer WebSocket/HTTP, ktory rejestruje klientow i posredniczy w streamingu plikow.
- `client` - klient, ktory indeksuje lokalny folder, laczy sie z serwerem i wysyla/pobiera pliki.
- `shared` - wspolne typy wiadomosci uzywane przez klienta i serwer.

## Bezpieczenstwo

Aktualna wersja wymaga tokenu autoryzacyjnego dla:

- polaczenia WebSocket: `/ws?token=...`
- uploadu streamu: `Authorization: Bearer <token>`
- downloadu streamu: `Authorization: Bearer <token>`

Serwer nie wypisuje tokenu w logach. Pliki `server_token.txt`, `client_token.txt`, `.env`, `shared_files/` i `target/` sa ignorowane przez git.

Klient dodatkowo sprawdza sciezki plikow przed wyslaniem:

- blokuje sciezki absolutne,
- blokuje `..`,
- blokuje odczyt spoza katalogu udostepniania po `canonicalize`.

Do uzycia poza lokalna siecia ustaw reverse proxy z TLS i korzystaj z `wss://`/`https://`. Token w adresie WebSocket moze trafic do logow proxy, dlatego w publicznym wdrozeniu logi URL powinny byc ograniczone.

## Wymagania

- Rust stable z Cargo
- Windows, Linux albo macOS

Instalacja Rust:

```bash
https://rustup.rs
```

## Budowanie i sprawdzanie

```bash
cargo check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

## Konfiguracja serwera

Serwer potrzebuje tokenu. Mozesz podac go przez zmienna srodowiskowa:

PowerShell:

```powershell
$env:SERVER_TOKEN="wklej_tutaj_dlugi_losowy_token"
cargo run -p server
```

Linux/macOS:

```bash
SERVER_TOKEN="wklej_tutaj_dlugi_losowy_token" cargo run -p server
```

Jesli `SERVER_TOKEN` nie jest ustawiony, serwer uzyje pliku `server_token.txt`. Gdy plik nie istnieje, wygeneruje nowy token i zapisze go w `server_token.txt`.

Domyslnie serwer nasluchuje na:

```text
0.0.0.0:5000
```

## Konfiguracja klienta

Klient potrzebuje tego samego tokenu co serwer.

Opcja 1: zmienna srodowiskowa:

PowerShell:

```powershell
$env:SERVER_TOKEN="ten_sam_token_co_na_serwerze"
$env:SERVER_URL="127.0.0.1:5000"
$env:SHARED_DIR="C:\sciezka\do\folderu"
cargo run -p client
```

Linux/macOS:

```bash
SERVER_TOKEN="ten_sam_token_co_na_serwerze" SERVER_URL="127.0.0.1:5000" SHARED_DIR="/home/user/pliki" cargo run -p client
```

Opcja 2: plik `client_token.txt` w katalogu glownym projektu:

```text
ten_sam_token_co_na_serwerze
```

Jesli `SHARED_DIR` nie jest ustawiony, klient uzyje folderu:

```text
./shared_files
```

Jesli `SERVER_URL` nie jest ustawiony, klient laczy sie z:

```text
127.0.0.1:5000
```

## Uruchomienie lokalne

1. Uruchom serwer:

```bash
cargo run -p server
```

2. Skopiuj token z `server_token.txt` do `client_token.txt` albo ustaw `SERVER_TOKEN`.

3. Utworz folder z plikami:

```bash
mkdir shared_files
```

4. Uruchom klienta:

```bash
cargo run -p client
```

5. Uruchom drugi klient na innym komputerze albo w innym katalogu roboczym z tym samym tokenem i `SERVER_URL` wskazujacym serwer.

## Zmienne srodowiskowe

- `SERVER_TOKEN` - token autoryzacyjny dla serwera i klienta.
- `SERVER_BIND` - adres nasluchiwania serwera, domyslnie `0.0.0.0:5000`.
- `SERVER_URL` - adres serwera dla klienta, domyslnie `127.0.0.1:5000`.
- `SHARED_DIR` - katalog udostepnianych plikow dla klienta, domyslnie `./shared_files`.
- `RUST_LOG` - poziom logowania, np. `debug`.

Przyklad:

```bash
RUST_LOG=debug cargo run -p server
```

## Uwagi operacyjne

- Nie commituj tokenow ani prywatnych plikow.
- Nie wystawiaj serwera publicznie bez TLS i kontroli logow.
- Kazdy klient z poprawnym tokenem moze prosic innych klientow o udostepnione pliki, wiec traktuj token jak sekret administracyjny.
