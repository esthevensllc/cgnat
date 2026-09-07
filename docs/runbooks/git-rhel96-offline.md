# Runbook de Git: instalación offline y actualización del portal

Fecha: 2026-09-07.

## 1. Entorno y alcance

- Servidor: RHEL 9.6 x86_64, host observado `LIMSTNFLOWV01`.
- Carpeta de trabajo: `/index2/portal-cgnat/releases/0.1.7`.
- Remoto: `http://172.19.245.139:8081/git/ellanos/portalcgnat.git`.
- Equipo de preparación: Windows con internet y Docker.
- Acceso operativo: VPN y escritorio remoto. No asumir acceso SSH/SCP directo desde Windows en este proyecto ni en procedimientos posteriores.
- Transferencia del paquete: correo o descarga web desde el repositorio. Git aún no está disponible para la transferencia inicial. Anteriormente se copiaba el código por WinSCP.

Los comandos Linux se ejecutan en la terminal del servidor. Los ejemplos usan la carpeta de la versión 0.1.7; adaptar explícitamente la ruta para otras versiones. Detenerse ante cualquier error antes de pasar al siguiente bloque.

## 2. Identificar el sistema

```bash
cat /etc/os-release
uname -m
git --version
```

Resultado inicial: RHEL 9.6, `x86_64` y `git: command not found`.

Inventario de dependencias:

```bash
rpm -q glibc libcurl libcurl-minimal expat pcre2 zlib openssl-libs openssh-clients
```

Versiones reportadas en este servidor:

| Paquete | Versión |
| --- | --- |
| glibc | 2.34-168.el9_6.19.x86_64 |
| libcurl | 7.76.1-31.el9.x86_64 |
| libcurl-minimal | No instalado; existe libcurl |
| expat | 2.5.0-5.el9_6.x86_64 |
| pcre2 | 10.40-6.el9.x86_64 |
| zlib | 1.2.11-40.el9.x86_64 |
| openssl-libs | 3.2.2-6.el9_5.1.x86_64 |
| openssh-clients | 8.7p1-45.el9.x86_64 |

## 3. Instalar Git offline mediante el paquete preparado

Paquete del proyecto: `output/git-offline-rhel96.zip` (aproximadamente 5,4 MB).

Incluye:

- `git-core-2.52.0-1.el9.x86_64.rpm`.
- `less-590-6.el9.x86_64.rpm`.
- `instalar.sh`, `SHA256SUMS` y `LEEME.txt`.

Los RPM proceden de los repositorios públicos UBI 9 de Red Hat, accesibles sin suscripción. El paquete aprovecha las dependencias existentes inventariadas arriba: no es un instalador universal para cualquier servidor RHEL. `git-core` proporciona los comandos habituales de Git, sin todas las utilidades y documentación del metapaquete `git`.

1. Transferir el ZIP por el canal disponible.
2. Extraerlo con la herramienta disponible. Si el servidor tiene `unzip`, ejecutar desde la carpeta que contiene el ZIP:

```bash
unzip git-offline-rhel96.zip
cd git-offline-rhel96
bash instalar.sh
```

Si no hay `unzip`, extraer en Windows y transferir la carpeta completa, conservando todos sus archivos. Ejecutar el script como `root`.

El script verifica SHA256, consulta las firmas, prueba dependencias y conflictos con `rpm --test`, y ejecuta DNF con los repositorios deshabilitados y comprobación de firmas locales activada. Solo incluye `less` en la transacción si aún no está instalado. Revisar la transacción y confirmar cuando DNF lo solicite.

```bash
git --version
```

Resultado esperado del paquete: `git version 2.52.0`.

Si faltan dependencias o claves de firma, conservar la salida completa para resolver el problema. No usar `--nodeps`, `--nogpgcheck` ni forzar la instalación.

### Preparación y validación realizadas en Windows

Se utilizó `registry.access.redhat.com/ubi9/ubi:9.6` con Docker para descargar `git-core` y sus dependencias usando DNF. Los repositorios públicos actuales de UBI 9 no están fijados a la versión menor 9.6; repetir la descarga puede producir otras versiones.

Se comprobaron las firmas Red Hat de Git y less y se probó la instalación en un contenedor UBI 9.6 sin red. Pasaron `git --version`, `git init`, un commit vacío y un clon local. El contenedor necesitó paquetes SSH adicionales; se excluyeron del ZIP destinado al servidor porque este ya dispone de `openssh-clients`. No instalar en el servidor los RPM auxiliares de la carpeta de preparación `output/git-offline-rhel9`.

La captura posterior del servidor confirma que Git funciona. No se ha comprobado desde este equipo la instalación ni el estado completo del servidor.

## 4. Inicializar y configurar el remoto

Si todavía no existe `.git` en la carpeta de la aplicación:

```bash
cd /index2/portal-cgnat/releases/0.1.7
git init
```

Si aparece `detected dubious ownership`, comprobar primero el propietario:

```bash
ls -ld /index2/portal-cgnat/releases/0.1.7
```

Preferir operar con el usuario propietario. Si se trabaja como `root` y se reconoce esta carpeta como confiable, autorizar únicamente esta ruta:

```bash
git config --global --add safe.directory /index2/portal-cgnat/releases/0.1.7
```

La excepción se guarda para el usuario que ejecuta el comando; no cambia propietarios ni permisos. No usar `safe.directory '*'`.

Consultar los remotos:

```bash
git remote -v
```

Si `origin` no existe:

```bash
git remote add origin http://172.19.245.139:8081/git/ellanos/portalcgnat.git
```

Si ya existe pero su URL es incorrecta:

```bash
git remote set-url origin http://172.19.245.139:8081/git/ellanos/portalcgnat.git
```

Verificar configuración y conectividad:

```bash
git remote -v
git ls-remote origin
```

`git remote add` requiere un nombre y una URL. `git ls-remote` consulta referencias sin modificar los archivos locales. Introducir las credenciales en el prompt si son necesarias, sin incluirlas en la URL ni en este documento. La instalación offline de Git no elimina la necesidad de conectividad con el repositorio interno para `fetch` o `pull`.

## 5. Vincular archivos previamente copiados por WinSCP

Aplicar esta sección solo al estado observado: `No commits yet` y archivos `Untracked` después de `git init`. No hacer un commit de todos los archivos del servidor como solución: puede incorporar configuración, datos o secretos locales.

### 5.1 Crear un respaldo completo

El respaldo incluye `.env`, `.git`, archivos ignorados y datos presentes en la carpeta. Comprobar espacio; si hay procesos escribiendo datos, coordinar una pausa o un respaldo consistente de esos datos. El TAR no respalda destinos externos de enlaces simbólicos ni garantiza por sí solo un respaldo de bases de datos.

```bash
cd /index2/portal-cgnat/releases
du -sh 0.1.7
df -h .
respaldo="0.1.7-respaldo-$(date +%Y%m%d-%H%M%S).tar.gz"
tar -czf "$respaldo" 0.1.7 && gzip -t "$respaldo" && ls -lh "$respaldo"
```

Continuar únicamente si termina sin errores. Guardar el nombre exacto del respaldo. Puede contener secretos; conservarlo en el servidor con acceso restringido.

### 5.2 Descargar referencias y elegir rama

```bash
cd /index2/portal-cgnat/releases/0.1.7
git fetch origin
git remote set-head origin -a
git symbolic-ref --short refs/remotes/origin/HEAD
```

La salida identifica la rama predeterminada, por ejemplo `origin/main`. Confirmar que es la rama que corresponde desplegar. Si `origin/HEAD` no se puede determinar:

```bash
git branch -r
```

Elegir la rama de despliegue existente y usar su referencia explícita en el siguiente bloque; no asumir que siempre es `main` o `master`.

### 5.3 Asociar el historial y revisar diferencias

Si la rama predeterminada es la correcta:

```bash
rama=$(git symbolic-ref --short refs/remotes/origin/HEAD) &&
git reset --mixed "$rama"
```

Si se requiere otra rama, asignar `rama=origin/NOMBRE_REAL` sustituyendo `NOMBRE_REAL` por una rama verificada, y luego ejecutar `git reset --mixed "$rama"`.

`reset --mixed` enlaza la rama local al commit remoto y actualiza el índice, conservando por ahora el contenido de los archivos. Revisar:

```bash
git status --short
git diff --stat
git ls-files -- .env storage
```

Si el remoto versiona `.env` o datos dentro de `storage`, el siguiente paso también puede sustituirlos. Resolver primero qué configuración o datos deben conservarse y asegurar su respaldo. Los archivos exclusivamente locales no versionados permanecen, aunque los conflictos entre archivos y directorios pueden impedir la operación.

### 5.4 Sustituir el código por la versión remota

Este bloque descarta las diferencias de WinSCP en archivos versionados y restaura los que falten. Ejecutarlo después del respaldo y de revisar la rama y los archivos afectados:

```bash
git restore --source=HEAD --worktree -- . &&
git branch --set-upstream-to="$rama"
```

Mantener la misma terminal para conservar la variable `rama`. La rama local puede seguir llamándose `master` y rastrear `origin/main`; no es necesario renombrarla para hacer pull.

```bash
git status
git branch -vv
git pull --ff-only
```

No usar `git clean` para eliminar archivos locales, ni `git reset --hard` como solución automática a errores.

## 6. Actualizaciones posteriores

```bash
cd /index2/portal-cgnat/releases/0.1.7
git status --short
git branch -vv
git fetch origin
git log --oneline HEAD..@{upstream}
git diff --stat HEAD @{upstream}
```

Si hay cambios locales, revisarlos y respaldarlos antes de continuar. Aplicar también el respaldo previo al despliegue según la sección 5.1.

```bash
git pull --ff-only
git status
git log -1 --oneline
```

`--ff-only` evita crear un merge automático cuando las ramas divergen. Si falla, revisar la divergencia; no forzar ni borrar cambios locales.

Actualizar Git no completa el despliegue de la aplicación. Aplicar el procedimiento del portal para dependencias, compilación, migraciones, cachés o reinicios según los cambios recibidos. No ejecutar esas acciones indiscriminadamente desde este runbook.

## 7. Recuperación

Si la vinculación o actualización produce un resultado incorrecto, detener nuevos cambios y registrar `git status` y `git log -1 --oneline`.

Para inspeccionar un respaldo, extraerlo en otra carpeta vacía, fuera del directorio activo. Sustituir la ruta del TAR por el archivo real:

```bash
recuperacion=$(mktemp -d /index2/portal-cgnat/releases/recuperacion-git-XXXXXX)
tar -xzf /ruta/real/al/respaldo.tar.gz -C "$recuperacion"
```

Comparar y restaurar selectivamente los archivos necesarios según el procedimiento de despliegue. No extraer automáticamente el respaldo sobre la aplicación en ejecución; también contiene la antigua carpeta `.git` y posibles datos que hayan cambiado desde el respaldo.

## 8. Errores observados

| Mensaje o situación | Acción |
| --- | --- |
| `git: command not found` | Instalar el ZIP offline y verificar `git --version`. |
| `not a git repository` | Entrar en la carpeta correcta y ejecutar `git init` solo si aún no existe repositorio. |
| `detected dubious ownership` | Verificar propietario; usar el propietario o autorizar la ruta concreta para el usuario operativo. |
| `remote origin already exists` | Consultar `git remote -v`; cambiar URL únicamente si es incorrecta. |
| `No commits yet` y archivos sin seguimiento | Seguir la vinculación inicial con respaldo de la sección 5. |
| Archivos sin seguimiento serían sobrescritos | No forzar el pull; revisar respaldo y procedimiento de vinculación. |
| Dependencias RPM ausentes | Conservar la salida; preparar las dependencias exactas, sin `--nodeps`. |
| Error de conexión al remoto | Comprobar acceso desde el servidor al host y puerto internos, además de autenticación. |
| `pull --ff-only` informa divergencia | Revisar commits locales y remotos antes de decidir cómo integrarlos. |

## 9. Alternativa con ISO: diagnóstico realizado

Se evaluó instalar desde la ISO DVD completa de RHEL 9.6 x86_64. No se encontró una ISO en las carpetas revisadas y `/dev/sr0` no mostró sistema de archivos o etiqueta detectables. Por ello se utilizó el paquete RPM público.

Comandos de diagnóstico:

```bash
find /root /home /tmp /var/tmp /opt /mnt /media -type f -iname '*.iso' 2>/dev/null
lsblk -o NAME,TYPE,SIZE,FSTYPE,LABEL,MOUNTPOINTS /dev/sr0
blkid /dev/sr0
```

Si en otra intervención se dispone de la ISO DVD real:

```bash
mkdir -p /mnt/rhel96
mount -o loop,ro /ruta/real/rhel-9.6-x86_64-dvd.iso /mnt/rhel96
```

O, si está cargada en la unidad virtual:

```bash
mount -o ro /dev/sr0 /mnt/rhel96
```

Comprobar que existen `BaseOS` y `AppStream` y ejecutar:

```bash
ls /mnt/rhel96
dnf --disablerepo='*' \
  --repofrompath=offline-baseos,file:///mnt/rhel96/BaseOS \
  --repofrompath=offline-appstream,file:///mnt/rhel96/AppStream \
  install git
git --version
```

La ruta `/ruta/real/...` es un marcador que debe sustituirse. Una Boot ISO no equivale al DVD completo. El error `failed to setup loop device` observado se produjo al intentar usar una ruta de ejemplo inexistente.

## 10. Fuentes y ubicación de artefactos

- [Repositorios y paquetes públicos UBI de Red Hat](https://access.redhat.com/articles/4238681).
- [RPM de Git en AppStream UBI 9](https://cdn-ubi.redhat.com/content/public/ubi/dist/ubi9/9/x86_64/appstream/os/Packages/g/).
- [Repositorio local desde DVD de RHEL](https://access.redhat.com/solutions/6913101).
- ZIP entregable: `output/git-offline-rhel96.zip`.
- Script y manifiesto: `output/git-offline-rhel96/`.

Estado al redactar: Git funciona en el servidor y se observó el repositorio local recién inicializado con archivos sin seguimiento. La vinculación y el primer pull se documentan como pasos pendientes de confirmar mediante la salida del servidor.
