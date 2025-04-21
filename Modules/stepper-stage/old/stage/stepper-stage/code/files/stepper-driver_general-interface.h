
class Stepper
{
public:
    Stepper(const int StepPin, const int DirPin);
}

class StepControl {
	
public:
	bool isOk() { return OK; }												// Return whether the controller (the PIT timer) started successfully
    bool isRunning() { return StepTimer.isRunning(); }						// Return whether the PIT timer is enabled and running.

    void move(Stepper& stepper0, float relSpeed = 1);
    void move(Stepper& stepper0, Stepper& stepper1, float relSpeed = 1);
    void move(Stepper& stepper0, Stepper& stepper1, Stepper& stepper2, float relSpeed = 1);
    template<size_t N> void move(Stepper* (&motors)[N], float relSpeed = 1);

    void moveAsync(Stepper& stepper0, float relSpeed = 1);
    void moveAsync(Stepper& stepper0, Stepper& stepper1, float relSpeed = 1);
    void moveAsync(Stepper& stepper0, Stepper& stepper1, Stepper& stepper2, float relSpeed = 1);
    template<size_t N> void moveAsync(Stepper* (&motors)[N], float relSpeed = 1);

    void rotateAsync(Stepper& stepper0, float relSpeed = 1);
    void rotateAsync(Stepper& stepper0, Stepper& stepper1, float relSpeed = 1);
    void rotateAsync(Stepper& stepper0, Stepper& stepper1, Stepper& stepper2, float relSpeed = 1);
    template<size_t N> void rotateAsync(Stepper* (&motors)[N], float relSpeed = 1);

    void stop();
    void stopAsync();
    // void emergencyStop() { StepTimer.disableInterupt(); is_stopping = false; }
    void emergencyStop() { StepTimer.disableInterupt();}
}

class Stepper
{
public:
    Stepper(const int StepPin, const int DirPin);

    Stepper& setStepPinPolarity(int p);

    Stepper& setInverseRotation(bool b);

    void setTargetAbs(int pos);			// Set target position 
    void setTargetRel(int delta);		// Set target position relative to current position
	void setDirPin(int dirCw);			// set the pin value in accordance with the require direction of motion
	void updateDirPin(void) {setDirPin(dirCw);}
	int32_t getCurrentAbsPosition(void) {return position + dirCw*current;}
	
	void prepareMotorToMove(void);

    inline int32_t getPosition() const { return position + dirCw* current; }	// update the position given the displacement of the current/last move
    inline void setPosition(int32_t pos) { position = pos; current = 0; }

	static constexpr int stepMaxMax = int(std::numeric_limits<int32_t>::max()/2)-1; //1073741824;	//2147483647
	static constexpr int stepMinMin = -stepMaxMax;	
	int stepMax = stepMaxMax;
	int stepMin = stepMinMin;
	inline void set_step_max(int step=stepMaxMax) {if (step>stepMin) stepMax = constrain_step(step, stepMinMin, stepMaxMax);}
	inline void set_step_min(int step=stepMinMin) {if (step<stepMax) stepMin = constrain_step(step, stepMinMin, stepMaxMax);}
	// int abs_target = 0;

    int direction;	// vMax sign. It's used only in StepControl.doRotate() and set in .setMaxSpeed()
}