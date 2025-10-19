import os
import win32crypt  
from cryptography.fernet import Fernet

SECRET_KEY_FILE = "secret.key"



def load_key():
    """
    Pega a chave encriptada do secret.key e usa a DPAPI para desencriptá-la.
    Só funciona na mesma conta(ou máquina?) onde a chave foi gerada.
    """
    with open(SECRET_KEY_FILE, "rb") as key_file:
        protected_key = key_file.read()
    # win32crypt.CryptUnprotectData devolve uma tupla: (descrição, dados)
    key = win32crypt.CryptUnprotectData(protected_key, None, None, None, 0)
    return key[1]



def decrypt_password(token, key=None):
    """
    Descripta o token dá return a senha original.
    """
    if key is None:
        key = load_key()
    f = Fernet(key)
    decrypted = f.decrypt(token.encode())
    return decrypted.decode()