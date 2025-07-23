import { Test, TestingModule } from '@nestjs/testing';
import { HydrologyRequestsController } from './hydrology-requests.controller';

describe('HydrologyRequestsController', () => {
  let controller: HydrologyRequestsController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [HydrologyRequestsController],
    }).compile();

    controller = module.get<HydrologyRequestsController>(
      HydrologyRequestsController,
    );
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
