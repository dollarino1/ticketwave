-- A user's privileges. Every existing account keeps working as an ordinary user.
ALTER TABLE users
    ADD COLUMN role TEXT NOT NULL DEFAULT 'user'
    CHECK (role IN ('user', 'organizer', 'admin'));
